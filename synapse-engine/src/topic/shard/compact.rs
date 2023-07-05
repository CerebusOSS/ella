use std::{
    collections::HashMap,
    io::{Cursor, Write},
    sync::Arc,
    time::Instant,
};

use datafusion::{
    datasource::file_format::parquet::fetch_parquet_metadata,
    parquet::{
        arrow::{
            arrow_reader::ParquetRecordBatchReader, arrow_to_parquet_schema,
            parquet_to_arrow_schema, AsyncArrowWriter,
        },
        column::writer::ColumnCloseResult,
        errors::ParquetError,
        file::{
            metadata::KeyValue,
            properties::{WriterProperties, WriterPropertiesPtr},
            reader::FileReader,
            serialized_reader::{ReadOptionsBuilder, SerializedFileReader},
            writer::{SerializedFileWriter, SerializedRowGroupWriter},
        },
        format::{ColumnIndex, FileMetaData, OffsetIndex},
        schema::types::TypePtr,
    },
};
use futures::{StreamExt, TryStreamExt};
use thrift::protocol::TSerializable;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};

use crate::{catalog::snapshot::ShardState, topic::config::ShardConfig, Schema, SynapseContext};

use super::ShardSet;

pub async fn compact_shards(
    sources: Vec<ShardState>,
    schema: Arc<Schema>,
    shards: Arc<ShardSet>,
    ctx: Arc<SynapseContext>,
    cfg: ShardConfig,
) -> crate::Result<()> {
    let start = Instant::now();
    let props = WriterProperties::builder()
        .set_sorting_columns(schema.sorting_columns())
        .set_max_row_group_size(cfg.row_group_size)
        .set_write_batch_size(cfg.write_batch_size)
        .build();

    let dst = shards
        .start_compact(
            sources.iter().map(|s| s.id).collect::<Vec<_>>(),
            (*schema).clone(),
        )
        .await?;

    let file_schema = schema
        .parquet_schema()
        .cloned()
        .unwrap_or_else(|| schema.arrow_schema().clone());

    let mut schema_changed = false;
    let mut combined_meta = HashMap::new();
    for src in &sources {
        let info = ctx.store().head(&src.path.as_path()).await?;
        let meta = fetch_parquet_metadata(&**ctx.store(), &info, Some(info.size)).await?;
        let arrow_schema = parquet_to_arrow_schema(
            meta.file_metadata().schema_descr(),
            meta.file_metadata().key_value_metadata(),
        )?;
        if arrow_schema != *file_schema {
            schema_changed = true;
            break;
        }
        if let Some(meta) = meta.file_metadata().key_value_metadata() {
            for entry in meta {
                combined_meta.insert(entry.key.clone(), entry.value.clone());
            }
        }
    }
    let (abort, file) = ctx.store().put_multipart(&dst.path.as_path()).await?;
    let res = if schema_changed {
        compact_new_schema(
            sources.clone(),
            file,
            schema,
            combined_meta,
            ctx.clone(),
            props,
        )
        .await
    } else {
        compact_same_schema(
            sources.clone(),
            file,
            schema,
            combined_meta,
            ctx.clone(),
            props,
        )
        .await
    };
    match res {
        Ok(rows) => {
            shards
                .finish_compact(
                    sources.iter().map(|s| s.id).collect::<Vec<_>>(),
                    dst.id,
                    rows,
                )
                .await?;
            ctx.store()
                .delete_stream(
                    futures::stream::iter(sources.into_iter().map(|s| Ok(s.path.as_path())))
                        .boxed(),
                )
                .try_collect::<Vec<_>>()
                .await?;
            let elapsed = (Instant::now() - start).as_secs_f64();
            tracing::debug!(rows, elapsed, "finished compacting shards");
        }
        Err(e) => {
            shards.delete_shard(dst.id).await?;
            ctx.store()
                .abort_multipart(&dst.path.as_path(), &abort)
                .await?;
            return Err(e);
        }
    }
    Ok(())
}

async fn compact_new_schema<W: AsyncWrite + Send + Unpin>(
    sources: Vec<ShardState>,
    dst: W,
    schema: Arc<Schema>,
    metadata: HashMap<String, Option<String>>,
    ctx: Arc<SynapseContext>,
    props: WriterProperties,
) -> crate::Result<usize> {
    let file_schema = schema
        .parquet_schema()
        .cloned()
        .unwrap_or_else(|| schema.arrow_schema().clone());
    let mut writer = AsyncArrowWriter::try_new(dst, file_schema, 1024 * 1024, Some(props))?;

    for (k, v) in metadata {
        writer.append_key_value_metadata(KeyValue::new(k, v));
    }

    for src in sources {
        let bytes = ctx.store().get(&src.path.as_path()).await?.bytes().await?;
        let len = bytes.len();
        let reader = ParquetRecordBatchReader::try_new(bytes, len)?;
        for batch in reader {
            let batch = batch?;
            writer.write(&batch).await?;
        }
    }

    let meta = writer.close().await?;
    Ok(meta.num_rows as usize)
}

async fn compact_same_schema<W: AsyncWrite + Send + Unpin>(
    sources: Vec<ShardState>,
    dst: W,
    schema: Arc<Schema>,
    metadata: HashMap<String, Option<String>>,
    ctx: Arc<SynapseContext>,
    props: WriterProperties,
) -> crate::Result<usize> {
    let file_schema = schema
        .parquet_schema()
        .cloned()
        .unwrap_or_else(|| schema.arrow_schema().clone());
    let parquet_schema = arrow_to_parquet_schema(&file_schema)?;
    let mut writer = AsyncParquetWriter::new(
        dst,
        parquet_schema.root_schema_ptr(),
        1024 * 1024,
        Arc::new(props),
    )?;

    for (k, v) in metadata {
        writer.append_key_value_metadata(KeyValue::new(k, v));
    }

    for src in sources {
        let bytes = ctx.store().get(&src.path.as_path()).await?.bytes().await?;
        let reader = SerializedFileReader::new_with_options(
            bytes.clone(),
            ReadOptionsBuilder::new().with_page_index().build(),
        )?;
        let file_meta = reader.metadata();
        let offset_index = file_meta.offset_index();

        for i in 0..reader.num_row_groups() {
            {
                let mut group_writer = writer.next_row_group()?;
                let group_reader = reader.get_row_group(i)?;
                let meta = group_reader.metadata();

                for (c, col) in meta.columns().iter().enumerate() {
                    let col_meta = meta.column(c);
                    let column_index = col_meta
                        .column_index_offset()
                        .zip(col_meta.column_index_length())
                        .map(|(off, len)| {
                            let range = (off as usize)..(off as usize + len as usize);
                            let mut d = Cursor::new(&bytes[range]);
                            let mut prot = thrift::protocol::TCompactInputProtocol::new(&mut d);
                            ColumnIndex::read_from_in_protocol(&mut prot)
                                .map_err(|err| ParquetError::External(Box::new(err)))
                        })
                        .transpose()?;

                    let (_, byte_len) = col.byte_range();
                    let close_res = ColumnCloseResult {
                        bytes_written: byte_len,
                        rows_written: meta.num_rows() as u64,
                        bloom_filter: group_reader.get_column_bloom_filter(c).cloned(),
                        column_index,
                        offset_index: offset_index.map(|idx| OffsetIndex {
                            page_locations: idx[i][c].clone(),
                        }),
                        metadata: col_meta.clone(),
                    };
                    group_writer.append_column(&bytes, close_res)?;
                }
                group_writer.close()?;
            }
            writer.flush().await?;
        }
    }

    let meta = writer.close().await?;

    Ok(meta.num_rows as usize)
}

struct AsyncParquetWriter<W> {
    sync_writer: SerializedFileWriter<SharedBuffer>,
    async_writer: W,
    shared_buffer: SharedBuffer,
}

impl<W> AsyncParquetWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    pub fn new(
        writer: W,
        schema: TypePtr,
        buffer_size: usize,
        properties: WriterPropertiesPtr,
    ) -> Result<Self, ParquetError> {
        let shared_buffer = SharedBuffer::new(buffer_size);
        let sync_writer = SerializedFileWriter::new(shared_buffer.clone(), schema, properties)?;
        Ok(Self {
            shared_buffer,
            sync_writer,
            async_writer: writer,
        })
    }

    pub fn next_row_group(
        &mut self,
    ) -> Result<SerializedRowGroupWriter<'_, SharedBuffer>, ParquetError> {
        self.sync_writer.next_row_group()
    }

    pub async fn close(mut self) -> Result<FileMetaData, ParquetError> {
        let meta = self.sync_writer.close()?;
        Self::try_flush(&mut self.shared_buffer, &mut self.async_writer, true).await?;
        self.async_writer.shutdown().await?;
        Ok(meta)
    }

    pub async fn flush(&mut self) -> Result<(), ParquetError> {
        Self::try_flush(&mut self.shared_buffer, &mut self.async_writer, false).await
    }

    pub fn append_key_value_metadata(&mut self, kv_metadata: KeyValue) {
        self.sync_writer.append_key_value_metadata(kv_metadata)
    }

    async fn try_flush(
        shared_buffer: &mut SharedBuffer,
        async_writer: &mut W,
        force: bool,
    ) -> Result<(), ParquetError> {
        let mut buffer = shared_buffer.buffer.try_lock().unwrap();
        if !force && buffer.len() < buffer.capacity() / 2 {
            return Ok(());
        }

        async_writer
            .write(buffer.as_slice())
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        async_writer
            .flush()
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        buffer.clear();
        Ok(())
    }
}

#[derive(Clone)]
struct SharedBuffer {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::flush(&mut *buffer)
    }
}
