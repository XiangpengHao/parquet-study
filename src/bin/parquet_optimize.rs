use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
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

    /// Enable or disable bloom filter (if not set, inherits from input file)
    #[arg(long)]
    bloom_filter: Option<bool>,

    /// Auto bloom filter: only enable bloom filters for byte array columns with smart NDV estimation
    #[arg(long)]
    auto_bloom_filter: bool,

    /// Bloom filter false positive probability (0.0 to 1.0, defaults to 0.05)
    #[arg(long)]
    bloom_filter_fpp: Option<f64>,

    /// Bloom filter number of distinct values (if not specified, defaults to a reasonable estimate based on row group sizes)
    #[arg(long)]
    bloom_filter_ndv: Option<u64>,
}

fn determine_statistics_level(metadata: &ParquetMetaData) -> (EnabledStatistics, bool) {
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

fn get_row_group_info(metadata: &ParquetMetaData) -> Vec<(usize, i64)> {
    let mut row_group_info = Vec::new();

    for i in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(i);
        let num_rows = row_group.num_rows();
        row_group_info.push((i, num_rows));
    }

    row_group_info
}

fn has_bloom_filters(metadata: &ParquetMetaData) -> bool {
    // Check if any row group has bloom filter metadata
    for i in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(i);
        for j in 0..row_group.num_columns() {
            let column_chunk = row_group.column(j);
            // Check if column chunk has bloom filter offset
            if column_chunk.bloom_filter_offset().is_some() {
                return true;
            }
        }
    }
    false
}

fn calculate_default_bloom_filter_ndv(row_group_info: &[(usize, i64)]) -> u64 {
    if row_group_info.is_empty() {
        return 10_000; // Minimal fallback
    }

    // Find the maximum row count across all row groups
    let max_rows = row_group_info
        .iter()
        .map(|(_, rows)| *rows)
        .max()
        .unwrap_or(0);

    // Use a conservative estimate: assume about 80% distinct values in the largest row group
    // This accounts for some duplicates while staying well below the row count
    let estimated_ndv = ((max_rows as f64) * 0.8) as u64;

    // Ensure we have reasonable bounds (at least 1000, at most 1M, never more than max_rows)
    let min_ndv = 1_000u64;
    let max_ndv = 1_000_000u64;
    let absolute_max = max_rows as u64;

    estimated_ndv.max(min_ndv).min(max_ndv).min(absolute_max)
}

fn estimate_ndv_from_row_size(metadata: &ParquetMetaData, column_index: usize) -> u64 {
    let mut total_bytes = 0i64;
    let mut total_rows = 0i64;

    // Sum up compressed size and row count across all row groups for this column
    for rg_idx in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(rg_idx);
        if column_index < row_group.num_columns() {
            let column_chunk = row_group.column(column_index);
            total_bytes += column_chunk.compressed_size();
            total_rows += row_group.num_rows();
        }
    }

    if total_rows == 0 {
        return 1000; // fallback
    }

    // Estimate average bytes per value
    let avg_bytes_per_value = total_bytes as f64 / total_rows as f64;

    // Heuristic: longer strings tend to be more unique
    // For short strings (< 10 bytes avg), assume 50% uniqueness
    // For medium strings (10-50 bytes), assume 70% uniqueness
    // For long strings (> 50 bytes), assume 85% uniqueness
    let uniqueness_factor = if avg_bytes_per_value < 10.0 {
        0.5
    } else if avg_bytes_per_value < 50.0 {
        0.7
    } else {
        0.85
    };

    let estimated_ndv = (total_rows as f64 * uniqueness_factor) as u64;

    // Ensure reasonable bounds
    estimated_ndv.max(100).min(total_rows as u64)
}

fn should_enable_bloom_filter_for_column(metadata: &ParquetMetaData, column_index: usize) -> bool {
    let schema_descr = metadata.file_metadata().schema_descr();
    if column_index < schema_descr.num_columns() {
        let column_descr = schema_descr.column(column_index);
        match column_descr.physical_type() {
            parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => true,
            _ => false,
        }
    } else {
        false
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

    // Read the input parquet file metadata
    let file = File::open(&args.input)?;
    let mut metadata_reader = ParquetMetaDataReader::new().with_page_indexes(true);
    metadata_reader.try_parse(&file)?;
    let metadata = metadata_reader.finish()?;

    // Get row group structure from original file
    let row_group_info = get_row_group_info(&metadata);
    println!("Original file has {} row groups:", row_group_info.len());
    for (rg_idx, num_rows) in &row_group_info {
        println!("  Row group {}: {} rows", rg_idx, num_rows);
    }

    // Determine the statistics level from the original file
    let (original_stats_level, has_offset_index) = determine_statistics_level(&metadata);
    println!("Original statistics level: {:?}", original_stats_level);

    // Determine column index setting from original file
    let has_column_index =
        metadata.column_index().is_some() && !metadata.column_index().unwrap().is_empty();

    // Determine bloom filter setting from original file
    let has_bloom_filter = has_bloom_filters(&metadata);

    // Handle auto bloom filter logic
    let use_bloom_filter = if args.auto_bloom_filter {
        println!("Auto bloom filter enabled - analyzing columns...");

        // Check which columns should get bloom filters
        let mut bloom_filter_columns = Vec::new();
        let schema_descr = metadata.file_metadata().schema_descr();
        for i in 0..schema_descr.num_columns() {
            if should_enable_bloom_filter_for_column(&metadata, i) {
                let column_descr = schema_descr.column(i);
                let estimated_ndv = estimate_ndv_from_row_size(&metadata, i);
                println!(
                    "  Column '{}' ({}): enabling bloom filter, estimated NDV: {}",
                    column_descr.name(),
                    column_descr.physical_type(),
                    estimated_ndv
                );
                bloom_filter_columns.push((i, estimated_ndv));
            } else {
                let column_descr = schema_descr.column(i);
                println!(
                    "  Column '{}' ({}): skipping (not byte array type)",
                    column_descr.name(),
                    column_descr.physical_type()
                );
            }
        }

        !bloom_filter_columns.is_empty()
    } else {
        args.bloom_filter.unwrap_or(has_bloom_filter)
    };

    // Use provided values or fall back to original file settings
    let use_offset_index = args.offset_index.unwrap_or(has_offset_index);
    let use_column_index = args.column_index.unwrap_or(has_column_index);

    // When column index is enabled, statistics level should be page
    let stats_level = if use_column_index {
        EnabledStatistics::Page
    } else {
        original_stats_level
    };

    println!("Using offset index: {}", use_offset_index);
    println!("Using column index: {}", use_column_index);
    println!("Using bloom filter: {}", use_bloom_filter);
    if use_bloom_filter && !args.auto_bloom_filter {
        let fpp = args.bloom_filter_fpp.unwrap_or(0.05);
        let ndv = args
            .bloom_filter_ndv
            .unwrap_or(calculate_default_bloom_filter_ndv(&row_group_info));
        println!("  Bloom filter FPP: {}", fpp);
        println!("  Bloom filter NDV: {}", ndv);
    }
    println!("Statistics level: {:?}", stats_level);

    // Open the input file again for reading data
    let input_file = File::open(&args.input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input_file)?;
    let schema = builder.schema().clone();

    // Create writer with specified compression and index settings
    let output_file = File::create(&output_path)?;
    let compression: Compression = args.compression.clone().into();
    let mut props_builder = WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(stats_level)
        .set_dictionary_page_size_limit(1024 * 1024 * 2)
        .set_offset_index_disabled(!use_offset_index);

    // Column index is enabled by default when statistics are page-level
    // We only need to disable it if explicitly requested
    if !use_column_index {
        // If column index is available via truncate length, set to None to disable
        props_builder = props_builder.set_column_index_truncate_length(None);
    }

    // Configure bloom filter settings
    if use_bloom_filter {
        if args.auto_bloom_filter {
            // Set up column-specific bloom filters
            let schema_descr = metadata.file_metadata().schema_descr();
            for i in 0..schema_descr.num_columns() {
                if should_enable_bloom_filter_for_column(&metadata, i) {
                    let estimated_ndv = estimate_ndv_from_row_size(&metadata, i);
                    let fpp = args.bloom_filter_fpp.unwrap_or(0.05);
                    let column_descr = schema_descr.column(i);
                    let column_path = parquet::schema::types::ColumnPath::from(column_descr.name());

                    props_builder = props_builder
                        .set_column_bloom_filter_enabled(column_path.clone(), true)
                        .set_column_bloom_filter_fpp(column_path.clone(), fpp)
                        .set_column_bloom_filter_ndv(column_path, estimated_ndv);
                }
            }
        } else {
            // Use global bloom filter settings
            props_builder = props_builder.set_bloom_filter_enabled(true);

            if let Some(fpp) = args.bloom_filter_fpp {
                props_builder = props_builder.set_bloom_filter_fpp(fpp);
            }

            let ndv = args
                .bloom_filter_ndv
                .unwrap_or_else(|| calculate_default_bloom_filter_ndv(&row_group_info));
            props_builder = props_builder.set_bloom_filter_ndv(ndv);
        }
    }

    let props = props_builder.build();
    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    // Copy data preserving row group structure
    let mut total_rows = 0;

    for (rg_idx, expected_rows) in row_group_info {
        println!(
            "Processing row group {} ({} rows)...",
            rg_idx, expected_rows
        );

        // Open a new file handle for each row group
        let input_file = File::open(&args.input)?;
        let row_group_builder = ParquetRecordBatchReaderBuilder::try_new(input_file)?;

        // Read only this specific row group
        let row_group_reader = row_group_builder.with_row_groups(vec![rg_idx]).build()?;

        let mut rg_rows = 0;
        for batch_result in row_group_reader {
            let batch = batch_result?;
            rg_rows += batch.num_rows();
            writer.write(&batch)?;
        }

        // Verify we read the expected number of rows
        if rg_rows as i64 != expected_rows {
            eprintln!(
                "Warning: Expected {} rows in row group {}, but read {}",
                expected_rows, rg_idx, rg_rows
            );
        }

        // Close the current row group in the writer
        writer.flush()?;
        total_rows += rg_rows;
    }

    writer.close()?;

    // Show file size comparison
    let input_size = std::fs::metadata(&args.input)?.len();
    let output_size = std::fs::metadata(&output_path)?.len();
    let compression_ratio = if output_size > input_size {
        // File got larger (negative compression)
        (output_size as f64 - input_size as f64) / input_size as f64 * -100.0
    } else {
        // File got smaller (positive compression)
        (input_size as f64 - output_size as f64) / input_size as f64 * 100.0
    };

    println!("Completed successfully!");
    println!("  Rows processed: {}", total_rows);
    println!("  Input size:     {} bytes", input_size);
    println!("  Output size:    {} bytes", output_size);
    if output_size > input_size {
        println!("  Size increase:  {:.1}%", compression_ratio.abs());
    } else {
        println!("  Compression:    {:.1}%", compression_ratio);
    }

    Ok(())
}
