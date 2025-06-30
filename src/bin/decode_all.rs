use arrow::datatypes::{DataType, Field, Schema};
use clap::Parser;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name = "decode_all")]
#[command(about = "Decode all batches from a parquet file and measure time")]
struct Args {
    /// Path to the parquet file
    #[arg(short, long)]
    file: String,

    /// Show detailed per-batch timing
    #[arg(short, long)]
    verbose: bool,

    /// Batch size for reading (default: use file's row group size)
    #[arg(short, long)]
    batch_size: Option<usize>,

    /// Enable flamegraph profiling
    #[arg(long)]
    flamegraph: bool,

    /// Read string/binary columns as view types (Utf8View/BinaryView)
    #[arg(long)]
    read_as_view: bool,

    /// Number of times to repeat the decoding process
    #[arg(short, long, default_value = "3")]
    repeat: usize,
}

/// Convert string and binary types to their view equivalents
fn convert_to_view_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| convert_field_to_view(field))
        .collect::<Vec<_>>();

    Schema::new(fields).with_metadata(schema.metadata().clone())
}

/// Recursively convert a field to use view types for strings and binaries
fn convert_field_to_view(field: &Field) -> Field {
    let new_data_type = convert_data_type_to_view(field.data_type());
    Field::new(field.name(), new_data_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

/// Convert a data type to use view types for strings and binaries
fn convert_data_type_to_view(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
        DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
        _ => data_type.clone(),
    }
}

/// Print file metadata information
fn print_metadata(builder: &ParquetRecordBatchReaderBuilder<File>, use_view_types: bool) {
    println!("File metadata:");
    println!(
        "  Number of row groups: {}",
        builder.metadata().num_row_groups()
    );
    println!("  Number of columns: {}", builder.schema().fields().len());
    println!(
        "  Total rows: {}",
        builder.metadata().file_metadata().num_rows()
    );
    if use_view_types {
        println!("  Schema converted to view types (Utf8View/BinaryView)");
    }
    println!();
}

/// Build a parquet reader with optional view schema conversion and batch size
fn build_reader(
    file: File,
    args: &Args,
) -> Result<
    impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    Box<dyn std::error::Error>,
> {
    let reader = if args.read_as_view {
        // Create reader with view schema conversion
        let temp_builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone()?)?;
        let view_schema = Arc::new(convert_to_view_schema(temp_builder.schema()));
        let options = ArrowReaderOptions::new().with_schema(view_schema);
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;

        print_metadata(&builder, true);

        match args.batch_size {
            Some(batch_size) => builder.with_batch_size(batch_size).build()?,
            None => builder.build()?,
        }
    } else {
        // Create reader with original schema
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        print_metadata(&builder, false);

        match args.batch_size {
            Some(batch_size) => builder.with_batch_size(batch_size).build()?,
            None => builder.build()?,
        }
    };

    Ok(reader)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Decoding parquet file: {}", args.file);
    println!("Will repeat {} times", args.repeat);

    // Print metadata once before starting iterations
    let file = File::open(&args.file)?;
    let temp_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    print_metadata(&temp_builder, args.read_as_view);

    let mut iteration_times = Vec::new();
    let mut total_rows = 0;
    let mut batch_count = 0;

    // Start profiling if flamegraph is enabled
    let guard = if args.flamegraph {
        Some(pprof::ProfilerGuard::new(100).unwrap())
    } else {
        None
    };

    for iteration in 1..=args.repeat {
        println!("=== Iteration {} ===", iteration);
        
        let file = File::open(&args.file)?;
        let reader = build_reader(file, &args)?;

        let iteration_start = Instant::now();
        let mut iteration_rows = 0;
        let mut iteration_batches = 0;

        // Decode all batches
        for batch_result in reader {
            let batch_start = Instant::now();
            let batch = batch_result?;
            let batch_duration = batch_start.elapsed();

            let num_rows = batch.num_rows();
            iteration_rows += num_rows;
            iteration_batches += 1;

            if args.verbose {
                println!(
                    "Batch {}: {} rows decoded in {:8.2}ms",
                    iteration_batches,
                    num_rows,
                    batch_duration.as_secs_f64() * 1000.0
                );
            }

            // Force materialization of all columns by accessing them
            for (col_idx, column) in batch.columns().iter().enumerate() {
                let _array_data = column.to_data();
                if args.verbose && iteration_batches == 1 && iteration == 1 {
                    println!(
                        "  Column {}: {} ({})",
                        col_idx,
                        batch.schema().field(col_idx).name(),
                        column.data_type()
                    );
                }
            }
        }

        let iteration_duration = iteration_start.elapsed();
        iteration_times.push(iteration_duration);
        
        // Update totals (use values from first iteration for consistency)
        if iteration == 1 {
            total_rows = iteration_rows;
            batch_count = iteration_batches;
        }

        println!(
            "Iteration {} complete: {:8.2}ms, {} rows, {:.0} rows/sec",
            iteration,
            iteration_duration.as_secs_f64() * 1000.0,
            iteration_rows,
            iteration_rows as f64 / iteration_duration.as_secs_f64()
        );
    }

    // Calculate statistics
    let total_time: f64 = iteration_times.iter().map(|d| d.as_secs_f64()).sum();
    let avg_time = total_time / iteration_times.len() as f64;
    let min_time = iteration_times.iter().map(|d| d.as_secs_f64()).fold(f64::INFINITY, f64::min);
    let max_time = iteration_times.iter().map(|d| d.as_secs_f64()).fold(0.0, f64::max);

    println!();
    println!("=== Summary ===");
    println!("  Total rows per iteration: {}", total_rows);
    println!("  Total batches per iteration: {}", batch_count);
    println!("  Iterations: {}", args.repeat);
    println!();
    println!("Timing results:");
    println!("  Min time: {:8.2}ms", min_time * 1000.0);
    println!("  Max time: {:8.2}ms", max_time * 1000.0);
    println!("  Avg time: {:8.2}ms", avg_time * 1000.0);
    println!(
        "  Avg rows per second: {:8.0}",
        total_rows as f64 / avg_time
    );

    // Generate flamegraph if profiling was enabled
    if let Some(guard) = guard {
        println!();
        println!("Generating flamegraph...");
        let report = guard.report().build().unwrap();
        let file = std::fs::File::create("decode_all_flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
        println!("Flamegraph saved to decode_all_flamegraph.svg");
    }

    Ok(())
}
