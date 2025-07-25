# Parquet study

A working-in-progress project to help people make most of Parquet (so that you don't need a hyped-up new format).

## Chapter 1: Metadata cache

So you want to cache Parquet metadata in DataFusion? (so that one Parquet metadata is read/decoded once and only once).

It's not easy (a blog post is coming soon), but not impossible.

## Usage

Copy the `src/metedata_cache.rs` to your project, and use it like below.
### Option 1 


```rust
use datafusion::prelude::*;
use crate::metadata_cache::RegisterParquetWithMetaCache;

let ctx = SessionContext::new();

// Instead of: 
// ctx.register_parquet("table", "file.parquet", ParquetReadOptions::default()).await?;
ctx.register_parquet_with_meta_cache(
    "table", 
    "path/to/file.parquet", 
    ParquetReadOptions::default()
).await?;
```

### Option 2

If you're low-level listing table users:

```rust
use crate::metadata_cache::{ParquetFormatMetadataCacheFactory, ToListingOptionsWithMetaCache};

let parquet_options = ParquetReadOptions::default();
let listing_options = parquet_options.to_listing_options_with_meta_cache(&ctx.copied_config(), ctx.copied_table_options());

ctx.register_listing_table(
    "table",
    "path/to/file.parquet",
    listing_options,
    parquet_options.schema.map(|s| Arc::new(s.to_owned())),
    None,
).await?;
```
