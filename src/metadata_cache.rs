use std::{
    any::Any,
    collections::HashMap,
    ops::Range,
    sync::{Arc, LazyLock},
};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::{Fields, Schema, SchemaRef},
    catalog::{Session, memory::DataSourceExec},
    common::{DEFAULT_PARQUET_EXTENSION, GetExt, Statistics},
    config::{ConfigField, ConfigFileType, TableOptions, TableParquetOptions},
    datasource::{
        file_format::{
            FileFormat, FileFormatFactory, FilePushdownSupport,
            file_compression_type::FileCompressionType,
            parquet::{
                ParquetFormat, statistics_from_parquet_meta_calc, transform_binary_to_string,
                transform_schema_to_view,
            },
        },
        listing::ListingOptions,
        physical_plan::{
            FileMeta, FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
            ParquetFileMetrics, ParquetFileReaderFactory, ParquetSource,
        },
    },
    error::{DataFusionError, Result},
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr, metrics::ExecutionPlanMetricsSet},
    prelude::{Expr, ParquetReadOptions, SessionConfig, SessionContext},
    sql::TableReference,
};
use futures::{FutureExt, future::BoxFuture};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataFetch, ParquetObjectReader},
        parquet_to_arrow_schema,
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use tokio::sync::RwLock;

pub static META_CACHE: LazyLock<MetadataCache> = LazyLock::new(MetadataCache::new);

pub struct MetadataCache {
    pub val: RwLock<HashMap<Path, Arc<ParquetMetaData>>>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self {
            val: RwLock::new(HashMap::new()),
        }
    }
}

pub struct ParquetMetadataCacheReader {
    file_metrics: ParquetFileMetrics,
    inner: ParquetObjectReader,
    path: Path,
}

impl AsyncFileReader for ParquetMetadataCacheReader {
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total as usize);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        self.inner.get_bytes(range)
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let path = self.path.clone();
        let options = options.cloned();
        async move {
            // First check with read lock
            {
                let cache = META_CACHE.val.read().await;
                if let Some(meta) = cache.get(&path) {
                    return Ok(meta.clone());
                }
            }

            // Upgrade to write lock and double-check
            let mut cache = META_CACHE.val.write().await;
            match cache.entry(path.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let meta = self.inner.get_metadata(options.as_ref()).await?;
                    let meta = Arc::try_unwrap(meta).unwrap_or_else(|e| e.as_ref().clone());
                    let mut reader = ParquetMetaDataReader::new_with_metadata(meta.clone())
                        .with_page_indexes(true);
                    reader.load_page_index(&mut self.inner).await?;
                    let meta = Arc::new(reader.finish()?);
                    entry.insert(meta.clone());
                    Ok(meta)
                }
            }
        }
        .boxed()
    }
}

/// A cached parquet file reader factory that creates ParquetMetadataCacheReader instances
#[derive(Debug)]
pub struct CachedParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
}

impl CachedParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);

        let store = Arc::clone(&self.store);
        let inner = parquet::arrow::async_reader::ParquetObjectReader::new(
            store,
            file_meta.object_meta.location.clone(),
        )
        .with_file_size(file_meta.object_meta.size);

        Ok(Box::new(ParquetMetadataCacheReader {
            file_metrics,
            inner,
            path: file_meta.object_meta.location,
        }))
    }
}

#[derive(Debug)]
pub struct ParquetFormatMetadataCacheFactory {
    options: Option<TableParquetOptions>,
}

impl ParquetFormatMetadataCacheFactory {
    pub fn new() -> Self {
        Self { options: None }
    }

    pub fn with_options(mut self, options: TableParquetOptions) -> Self {
        self.options = Some(options);
        self
    }
}

impl FileFormatFactory for ParquetFormatMetadataCacheFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let parquet_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::PARQUET);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.parquet
            }
            Some(parquet_options) => {
                let mut parquet_options = parquet_options.clone();
                for (k, v) in format_options {
                    parquet_options.set(k, v)?;
                }
                parquet_options
            }
        };
        let parquet_format = ParquetFormat::new().with_options(parquet_options);
        Ok(Arc::new(ParquetFormatMetadataCache::new(parquet_format)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        let parquet_format = ParquetFormat::new();
        Arc::new(ParquetFormatMetadataCache::new(parquet_format))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for ParquetFormatMetadataCacheFactory {
    fn get_ext(&self) -> String {
        DEFAULT_PARQUET_EXTENSION[1..].to_string()
    }
}

/// A wrapper around ParquetFormat that implements metadata caching for schema inference
/// and uses CachedParquetFileReaderFactory for physical plan creation
#[derive(Debug)]
pub struct ParquetFormatMetadataCache {
    inner: ParquetFormat,
}

impl ParquetFormatMetadataCache {
    fn new(inner: ParquetFormat) -> Self {
        Self { inner }
    }

    async fn get_cached_schema(&self, file: &ObjectMeta) -> Option<Schema> {
        let cache = META_CACHE.val.read().await;
        if let Some(metadata) = cache.get(&file.location) {
            let file_metadata = metadata.file_metadata();
            match parquet_to_arrow_schema(
                file_metadata.schema_descr(),
                file_metadata.key_value_metadata(),
            ) {
                Ok(schema) => Some(schema),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    async fn get_cached_stats(
        &self,
        file: &ObjectMeta,
        table_schema: SchemaRef,
    ) -> Option<Statistics> {
        let cache = META_CACHE.val.read().await;
        if let Some(metadata) = cache.get(&file.location) {
            match statistics_from_parquet_meta_calc(metadata.as_ref(), table_schema) {
                Ok(stats) => Some(stats),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    async fn fetch_schema_with_cache(
        &self,
        store: &dyn ObjectStore,
        file: &ObjectMeta,
    ) -> Result<Schema> {
        if let Some(schema) = self.get_cached_schema(file).await {
            return Ok(schema);
        }

        let metadata = fetch_parquet_metadata(store, file, self.inner.metadata_size_hint()).await?;

        // Insert the fetched metadata into cache
        {
            let mut cache = META_CACHE.val.write().await;
            cache
                .entry(file.location.clone())
                .or_insert_with(|| Arc::new(metadata.clone()));
        }

        let file_metadata = metadata.file_metadata();
        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?;

        Ok(schema)
    }

    async fn fetch_stats_with_cache(
        &self,
        store: &dyn ObjectStore,
        table_schema: SchemaRef,
        file: &ObjectMeta,
    ) -> Result<Statistics> {
        if let Some(stats) = self.get_cached_stats(file, table_schema.clone()).await {
            return Ok(stats);
        }

        let metadata = fetch_parquet_metadata(store, file, self.inner.metadata_size_hint()).await?;

        // Insert the fetched metadata into cache
        {
            let mut cache = META_CACHE.val.write().await;
            cache
                .entry(file.location.clone())
                .or_insert_with(|| Arc::new(metadata.clone()));
        }

        let stats = statistics_from_parquet_meta_calc(&metadata, table_schema)?;
        Ok(stats)
    }
}

#[async_trait]
impl FileFormat for ParquetFormatMetadataCache {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas: Vec<_> = Vec::new();

        for object in objects {
            let schema = self.fetch_schema_with_cache(store.as_ref(), object).await?;
            schemas.push((object.location.clone(), schema));
        }

        schemas.sort_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas
            .into_iter()
            .map(|(_, schema)| schema)
            .collect::<Vec<_>>();

        let schema = if self.inner.skip_metadata() {
            Schema::try_merge(clear_metadata(schemas))
        } else {
            Schema::try_merge(schemas)
        }?;

        let schema = if self.inner.binary_as_string() {
            transform_binary_to_string(&schema)
        } else {
            schema
        };

        let schema = if self.inner.force_view_types() {
            transform_schema_to_view(&schema)
        } else {
            schema
        };

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.fetch_stats_with_cache(store.as_ref(), table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut predicate = None;
        let mut metadata_size_hint = None;

        // Same predicate logic as original
        if self.inner.enable_pruning() {
            if let Some(pred) = filters.cloned() {
                predicate = Some(pred);
            }
        }
        if let Some(metadata) = self.inner.metadata_size_hint() {
            metadata_size_hint = Some(metadata);
        }

        let mut source = ParquetSource::new(self.inner.options().clone());

        if let Some(predicate) = predicate {
            source = source.with_predicate(Arc::clone(&conf.file_schema), predicate);
        }
        if let Some(metadata_size_hint) = metadata_size_hint {
            source = source.with_metadata_size_hint(metadata_size_hint);
        }

        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;
        let cached_reader_factory = Arc::new(CachedParquetFileReaderFactory::new(object_store));
        source = source.with_parquet_file_reader_factory(cached_reader_factory);

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();

        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        file_schema: &Schema,
        table_schema: &Schema,
        filters: &[&Expr],
    ) -> Result<FilePushdownSupport> {
        // Delegate to inner implementation
        self.inner
            .supports_filters_pushdown(file_schema, table_schema, filters)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        self.inner.file_source()
    }
}

fn clear_metadata(schemas: impl IntoIterator<Item = Schema>) -> impl Iterator<Item = Schema> {
    schemas.into_iter().map(|schema| {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                field.as_ref().clone().with_metadata(Default::default()) // clear meta
            })
            .collect::<Fields>();
        Schema::new(fields)
    })
}

pub trait ToListingOptionsWithMetaCache {
    fn to_listing_options_with_meta_cache(
        &self,
        config: &SessionConfig,
        table_options: TableOptions,
    ) -> ListingOptions;
}

impl ToListingOptionsWithMetaCache for ParquetReadOptions<'_> {
    fn to_listing_options_with_meta_cache(
        &self,
        config: &SessionConfig,
        table_options: TableOptions,
    ) -> ListingOptions {
        let mut file_format = ParquetFormat::new().with_options(table_options.parquet);

        if let Some(parquet_pruning) = self.parquet_pruning {
            file_format = file_format.with_enable_pruning(parquet_pruning)
        }
        if let Some(skip_metadata) = self.skip_metadata {
            file_format = file_format.with_skip_metadata(skip_metadata)
        }

        let new_format = ParquetFormatMetadataCache::new(file_format);
        ListingOptions::new(Arc::new(new_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(config.target_partitions())
            .with_table_partition_cols(self.table_partition_cols.clone())
            .with_file_sort_order(self.file_sort_order.clone())
    }
}

pub trait RegisterParquetWithMetaCache {
    #[allow(async_fn_in_trait)]
    async fn register_parquet_with_meta_cache(
        &self,
        table_ref: impl Into<TableReference>,
        table_path: impl AsRef<str>,
        options: ParquetReadOptions<'_>,
    ) -> Result<()>;
}

impl RegisterParquetWithMetaCache for SessionContext {
    async fn register_parquet_with_meta_cache(
        &self,
        table_ref: impl Into<TableReference>,
        table_path: impl AsRef<str>,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options
            .to_listing_options_with_meta_cache(&self.copied_config(), self.copied_table_options());

        self.register_listing_table(
            table_ref,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;

        Ok(())
    }
}

/// [`MetadataFetch`] adapter for reading bytes from an [`ObjectStore`]
struct ObjectStoreFetch<'a> {
    store: &'a dyn ObjectStore,
    meta: &'a ObjectMeta,
}

impl<'a> ObjectStoreFetch<'a> {
    fn new(store: &'a dyn ObjectStore, meta: &'a ObjectMeta) -> Self {
        Self { store, meta }
    }
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        async {
            self.store
                .get_range(&self.meta.location, range)
                .await
                .map_err(ParquetError::from)
        }
        .boxed()
    }
}

pub async fn fetch_parquet_metadata(
    store: &dyn ObjectStore,
    meta: &ObjectMeta,
    size_hint: Option<usize>,
) -> Result<ParquetMetaData> {
    let file_size = meta.size;
    let fetch = ObjectStoreFetch::new(store, meta);

    ParquetMetaDataReader::new()
        .with_prefetch_hint(size_hint)
        .with_column_indexes(true)
        .with_offset_indexes(true)
        .load_and_finish(fetch, file_size)
        .await
        .map_err(DataFusionError::from)
}
