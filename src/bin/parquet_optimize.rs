use clap::{Parser, ValueEnum};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone, ValueEnum)]
enum CompressionAlgorithm {
    None,
    Snappy,
    Lz4,
    Zstd,
}

impl From<CompressionAlgorithm> for Compression {
    fn from(algo: CompressionAlgorithm) -> Self {
        match algo {
            CompressionAlgorithm::None => Compression::UNCOMPRESSED,
            CompressionAlgorithm::Snappy => Compression::SNAPPY,
            CompressionAlgorithm::Lz4 => Compression::LZ4,
            CompressionAlgorithm::Zstd => Compression::ZSTD(Default::default()),
        }
    }
}

#[derive(Parser)]
#[command(name = "parquet_recompress")]
#[command(about = "Recompress parquet files with different compression algorithms")]
struct Args {
    /// Input parquet file path
    #[arg(short, long)]
    input: String,

    /// Compression algorithm to use
    #[arg(short, long, value_enum)]
    compression: CompressionAlgorithm,

    /// Output file path (optional, defaults to input with compression suffix)
    #[arg(short, long)]
    output: Option<String>,

    /// Enable or disable offset index (if not set, inherits from input file)
    #[arg(long)]
    offset_index: Option<bool>,

    /// Enable or disable column index (if not set, inherits from input file)
    #[arg(long)]
    column_index: Option<bool>,
}

fn determine_statistics_level(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> (parquet::file::properties::EnabledStatistics, bool) {
    use parquet::file::properties::EnabledStatistics;

    // Check if there are any row groups
    if metadata.num_row_groups() == 0 {
        return (EnabledStatistics::None, false);
    }

    // Check the first row group to determine statistics level
    let row_group = metadata.row_group(0);

    // Check if any column chunk has statistics
    let has_chunk_stats = (0..row_group.num_columns()).any(|i| {
        if let Some(column_chunk) = row_group.column(i).statistics() {
            // Check if statistics have meaningful values (not just defaults)
            column_chunk.min_bytes_opt().is_some() || column_chunk.max_bytes_opt().is_some()
        } else {
            false
        }
    });

    if !has_chunk_stats {
        return (EnabledStatistics::None, false);
    }

    // Check if page-level statistics exist by looking at column indexes
    // Note: This is a simplified check. In practice, page-level statistics
    // are stored in separate column index structures
    let has_page_stats =
        metadata.column_index().is_some() && !metadata.column_index().unwrap().is_empty();

    let has_offset_index =
        metadata.offset_index().is_some() && !metadata.offset_index().unwrap().is_empty();

    if has_page_stats {
        (EnabledStatistics::Page, has_offset_index)
    } else {
        (EnabledStatistics::Chunk, has_offset_index)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let input_path = Path::new(&args.input);
    if !input_path.exists() {
        eprintln!("Error: Input file '{}' not found", args.input);
        std::process::exit(1);
    }

    // Generate output filename if not provided
    let output_path = match args.output {
        Some(path) => path,
        None => {
            let stem = input_path.file_stem().unwrap().to_str().unwrap();
            let compression_suffix = match args.compression {
                CompressionAlgorithm::None => "uncompressed",
                CompressionAlgorithm::Snappy => "snappy",
                CompressionAlgorithm::Lz4 => "lz4",
                CompressionAlgorithm::Zstd => "zstd",
            };
            format!("{}_{}.parquet", stem, compression_suffix)
        }
    };

    println!(
        "Rewriting '{}' to '{}' using {:?}",
        args.input, output_path, args.compression
    );

    // Read the input parquet file
    let file = File::open(&args.input)?;
    let mut metadata_reader = ParquetMetaDataReader::new().with_page_indexes(true);
    metadata_reader.try_parse(&file)?;
    let metadata = metadata_reader.finish()?;

    // Determine the statistics level from the original file
    let (original_stats_level, has_offset_index) = determine_statistics_level(&metadata);
    println!("Original statistics level: {:?}", original_stats_level);

    // Determine column index setting from original file
    let has_column_index =
        metadata.column_index().is_some() && !metadata.column_index().unwrap().is_empty();

    // Use provided values or fall back to original file settings
    let use_offset_index = args.offset_index.unwrap_or(has_offset_index);
    let use_column_index = args.column_index.unwrap_or(has_column_index);

    // When column index is enabled, statistics level should be page
    let stats_level = if use_column_index {
        parquet::file::properties::EnabledStatistics::Page
    } else {
        original_stats_level
    };

    println!("Using offset index: {}", use_offset_index);
    println!("Using column index: {}", use_column_index);
    println!("Statistics level: {:?}", stats_level);

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    // Create writer with specified compression and index settings
    let output_file = File::create(&output_path)?;
    let compression: Compression = args.compression.clone().into();
    let mut props_builder = WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(stats_level)
        .set_offset_index_disabled(!use_offset_index);

    // Column index is enabled by default when statistics are page-level
    // We only need to disable it if explicitly requested
    if !use_column_index {
        // If column index is available via truncate length, set to None to disable
        props_builder = props_builder.set_column_index_truncate_length(None);
    }

    let props = props_builder.build();

    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    // Copy data with new compression
    let mut total_rows = 0;
    for batch_result in reader {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        writer.write(&batch)?;
    }

    writer.close()?;

    // Show file size comparison
    let input_size = std::fs::metadata(&args.input)?.len();
    let output_size = std::fs::metadata(&output_path)?.len();
    let compression_ratio = (input_size as f64 - output_size as f64) / input_size as f64 * 100.0;

    println!("Completed successfully!");
    println!("  Rows processed: {}", total_rows);
    println!("  Input size:     {} bytes", input_size);
    println!("  Output size:    {} bytes", output_size);
    println!("  Compression:    {:.1}%", compression_ratio);

    Ok(())
}
