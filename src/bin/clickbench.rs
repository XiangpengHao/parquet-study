use clap::Parser;
use datafusion::prelude::*;
use std::time::Instant;

use parquet_study::metadata_cache::RegisterParquetWithMetaCache;

#[derive(Parser, Debug)]
#[command(name = "clickbench")]
#[command(about = "ClickBench benchmark using DataFusion")]
struct Args {
    /// Optional query ID (1-based). If not provided, runs all queries
    #[arg(long)]
    query: Option<usize>,

    /// Path to the benchmark data file
    #[arg(long)]
    file: String,

    /// Path to the SQL queries file
    #[arg(long, default_value = "benchmark/clickbench.sql")]
    sql_path: String,

    /// Enable metadata cache
    #[arg(long)]
    metadata_cache: bool,

    /// Enable flamegraph
    #[arg(long)]
    flamegraph: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("{:?}", args);

    let data_path = std::env::current_dir()?.join(&args.file);

    let ctx = SessionContext::new();
    if args.metadata_cache {
        ctx.register_parquet_with_meta_cache(
            "hits",
            data_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
    } else {
        ctx.register_parquet(
            "hits",
            data_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
    }

    let sql_content = std::fs::read_to_string(&args.sql_path)?;
    let queries: Vec<&str> = sql_content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();

    let iterations = 3;

    let guard = if args.flamegraph {
        Some(pprof::ProfilerGuard::new(100).unwrap())
    } else {
        None
    };

    let start = Instant::now();
    match args.query {
        Some(id) if id > 0 && id <= queries.len() => {
            run_query(&ctx, queries[id - 1], id, iterations).await?;
        }
        Some(id) => {
            eprintln!(
                "Error: Query ID {} is out of range (1-{})",
                id,
                queries.len()
            );
            std::process::exit(1);
        }
        None => {
            println!("Running {} ClickBench queries...\n", queries.len());
            for (i, query) in queries.iter().enumerate() {
                run_query(&ctx, query, i + 1, iterations).await?;
            }
        }
    }
    let duration = start.elapsed();
    println!("Total time: {:8.2}ms", duration.as_secs_f64() * 1000.0);

    if let Some(guard) = guard {
        let report = guard.report().build().unwrap();
        let file = std::fs::File::create("flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    }

    Ok(())
}

async fn run_query(
    ctx: &SessionContext,
    sql: &str,
    query_id: usize,
    iterations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..iterations {
        run_query_once(ctx, sql, query_id).await?;
    }

    Ok(())
}

async fn run_query_once(
    ctx: &SessionContext,
    sql: &str,
    query_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Q{:2}: {:20} ", query_id, sql);

    let start = Instant::now();

    match ctx.sql(sql).await?.collect().await {
        Ok(results) => {
            let duration = start.elapsed();
            let row_count = results.iter().map(|batch| batch.num_rows()).sum::<usize>();
            println!(
                "{:8.2}ms ({} rows)",
                duration.as_secs_f64() * 1000.0,
                row_count
            );
        }
        Err(e) => {
            let duration = start.elapsed();
            println!("{:8.2}ms ERROR: {}", duration.as_secs_f64() * 1000.0, e);
        }
    }
    Ok(())
}
