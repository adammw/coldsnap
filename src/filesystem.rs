use std::cell::RefCell;
use std::collections::HashMap;
use aws_sdk_ebs::Client as EbsClient;
use positioned_io2::{ReadAt, Size};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::path::Path;
use sha2::{Digest, Sha256};
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::io::{ErrorKind, SeekFrom, stdout, Write};
use std::ops::{Deref, Index};
use std::time::Duration;
use futures::stream::{self, StreamExt, TryStreamExt};
use super::download::{SnapshotDownloader, Snapshot};
use bytes::{Buf, Bytes};
use stretto::{AsyncCache, CacheError};
use color_eyre::eyre;

use ext4_rs_nom::Ext4Volume;

use gpt;
use gpt::GptError;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub struct Error(error::Error);

const SNAPSHOT_BLOCK_WORKERS: usize = 64;
const SNAPSHOT_BLOCK_ATTEMPTS: u8 = 3;
const SHA256_ALGORITHM: &str = "SHA256";
const GIBIBYTE: i64 = 1024 * 1024 * 1024;

impl From<super::download::Error> for Error {
    fn from(err: super::download::Error) -> Self {
        Error::from(error::Error::DownloadError { source: err })
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::from(error::Error::StdIoError { source: err })
    }
}

impl From<uuid::Error> for Error {
    fn from(err: uuid::Error) -> Self {
        Error::from(error::Error::UuidError { source: err })
    }
}

impl From<GptError> for Error {
    fn from(err: GptError) -> Self {
        Error::from(error::Error::GptError { source: err })
    }
}

impl From<CacheError> for Error {
    fn from(err: CacheError) -> Self {
        Error::from(error::Error::CacheError { source: err })
    }
}

impl From<eyre::Report> for Error {
    fn from(err: eyre::Report) -> Self {
        Error::from(error::Error::EyreError { source: err })
    }
}

impl From<Elapsed> for Error {
    fn from(err: Elapsed) -> Self { Error::from(error::Error::TimeoutError { source: err })}
}

type Result<T> = std::result::Result<T, Error>;

pub struct SnapshotFilesystem {
    ebs_client: EbsClient,
}

impl SnapshotFilesystem {
    pub fn new(ebs_client : EbsClient) -> Self {
        SnapshotFilesystem { ebs_client }
    }

    async fn extract_volume(
        &self,
        snapshot_id: &str,
    ) -> Result<Ext4Volume<SnapshotReader>> {
        let snapshot_reader = SnapshotReader::new(self.ebs_client.clone(), snapshot_id).await?;

        // only read initial part of disk to avoid loading entire disk just to read backup header
        let gpt_reader = snapshot_reader.with_offset(0, Some(4096 * 33));
        let disk = gpt::GptConfig::new().writable(false).open_from_device(Box::new(gpt_reader))?;
        // println!("Disk (primary) header: {:#?}", disk.primary_header());
        // println!("Partition layout: {:#?}", disk.partitions());

        let ext4Uuid = Uuid::parse_str("0FC63DAF-8483-4772-8E79-3D69D8477DE4")?;

        let (_, partition) = disk
            .partitions()
            .iter()
            .find(|(_, p)| p.part_type_guid.guid == ext4Uuid)
            .ok_or(error::Error::PartitionError {})?;

        let partition_start = partition.first_lba * u64::from(*disk.logical_block_size());
        let ext4volume = Ext4Volume::new(snapshot_reader.with_offset(partition_start, None))?;
        Ok(ext4volume)
    }

    /// Download a snapshot into the file at the specified path.
    /// * `snapshot_id` is the snapshot to download.
    /// * `path` is the path prefix to start listing files from
    pub async fn list_files<P: AsRef<Path>>(
        &self,
        snapshot_id: &str,
        path: P,
    ) -> Result<()> {
        let base_path = path.as_ref().display().to_string();

        let ext4volume = self.extract_volume(snapshot_id).await?;

        let root = ext4volume.resolve_path(base_path.as_str())?;
        ext4volume.walk(&root, "/", &mut |_, path, _| {
            if path != "/" {
                println!("{}{}", base_path, path);
            }
            Ok(true)
        })?;

        Ok(())
    }

    pub async fn read_file<P: AsRef<Path>>(
        &self,
        snapshot_id: &str,
        path: P,
    ) -> Result<()> {

        let path = path.as_ref().display().to_string();
        let ext4volume = self.extract_volume(snapshot_id).await?;

        let inode = ext4volume.resolve_path(path.as_str())?;
        for mut data in ext4volume.data(&inode)? {
            let mut out = stdout();
            let mut cursor = positioned_io2::Cursor::new(&mut data);
            std::io::copy(&mut cursor, &mut out).expect("TODO: panic message");
        }

        Ok(())
    }
}

/// Stores the context needed to download a snapshot block.
struct BlockContext<'a> {
    output_buffer: &'a mut [u8],
    block_index: i32,
    block_token: String,
    block_size: i32,
    snapshot_id: String,
    ebs_client: EbsClient,
    cache: &'a AsyncCache<i32, Bytes>
}

/// Download a single block from the snapshot in context and write it to the file.
async fn download_block(context: &BlockContext<'_>) -> Result<Bytes> {
    let ebs_client = &context.ebs_client;
    let snapshot_id = &context.snapshot_id;
    let block_index = context.block_index;
    let block_token = &context.block_token;
    let block_size = context.block_size;

    if let Some(val) = context.cache.get(&block_index) {
        return Ok(val.as_ref().clone())
    }

    // TODO: block mutal downloads from happening
    eprintln!("Download block 0x{:x}", block_index);
    let mut retryAttempts = 3;

    let response = loop {
        retryAttempts -= 1;
        let response = ebs_client
            .get_snapshot_block()
            .snapshot_id(snapshot_id)
            .block_index(block_index)
            .block_token(block_token)
            .send();
        let timeoutResp = timeout(Duration::from_secs(2), response).await;
        match timeoutResp {
            Ok(resp) => {
                break resp
                .context(error::GetSnapshotBlockSnafu {
                    snapshot_id,
                    block_index,
                });
            }
            Err(e) => {
                println!("timeout, attempts remaining {}", retryAttempts);
                if retryAttempts <= 0 {
                    break Err(error::Error::TimeoutError{ source: e });
                }
            }
        }
    }?;
    println!("download block 0x{:x} done", block_index);

    let expected_hash = response.checksum.context(error::FindBlockPropertySnafu {
        snapshot_id,
        block_index,
        property: "checksum",
    })?;

    let checksum_algorithm = response
        .checksum_algorithm
        .context(error::FindBlockPropertySnafu {
            snapshot_id,
            block_index,
            property: "checksum algorithm",
        })?
        .as_str()
        .to_string();

    let data_length = response
        .data_length
        .context(error::FindBlockPropertySnafu {
            snapshot_id,
            block_index,
            property: "data length",
        })?;

    let block_data_stream =
        response
            .block_data
            .collect()
            .await
            .context(error::CollectByteStreamSnafu {
                snapshot_id,
                block_index,
                property: "data",
            })?;

    let block_data = block_data_stream.into_bytes();

    ensure!(
        checksum_algorithm == SHA256_ALGORITHM,
        error::UnexpectedBlockChecksumAlgorithmSnafu {
            snapshot_id,
            block_index,
            checksum_algorithm,
        }
    );
    let block_data_length = block_data.len();
    let block_data_length =
        i32::try_from(block_data_length).with_context(|_| error::ConvertNumberSnafu {
            what: "block data length",
            number: block_data_length.to_string(),
            target: "i32",
        })?;

    ensure!(
        data_length > 0 && data_length <= block_size && data_length == block_data_length,
        error::UnexpectedBlockDataLengthSnafu {
            snapshot_id,
            block_index,
            data_length,
        }
    );

    let mut block_digest = Sha256::new();
    block_digest.update(&block_data);
    let hash_bytes = block_digest.finalize();
    let block_hash = base64::encode(&hash_bytes);

    ensure!(
        block_hash == expected_hash,
        error::BadBlockChecksumSnafu {
            snapshot_id,
            block_index,
            block_hash,
            expected_hash,
        }
    );

    context.cache.insert(block_index, block_data.clone(), 1).await;

    Ok(block_data)
}

struct SnapshotReader {
    ebs_client: EbsClient,
    snapshot: Snapshot,
    block_map: RefCell<StreamingBlockMap>,
    internal_offset: u64,
    cur_offset: u64,
    max_offset: Option<u64>,
    cache: AsyncCache<i32, Bytes>,
}

#[derive(Clone)]
struct StreamingBlockMap {
    block_map: HashMap<i32, String>,
    block_map_complete: bool,
    next_token: Option<String>,
}

impl SnapshotReader {
    pub async fn new(ebs_client: EbsClient, snapshot_id: &str) -> Result<Self> {
        let downloader = SnapshotDownloader::new( ebs_client.clone());

        // preload snapshot metadata and first 100 blocks
        let snapshot = downloader.list_snapshot_blocks(snapshot_id, Some(100)).await?;
        let block_map = StreamingBlockMap {
            block_map: snapshot.blocks.iter().map(|x| (x.index, x.token.clone())).collect(),
            block_map_complete: false,
            next_token: None,
        };

        let cache: AsyncCache<i32, Bytes> = AsyncCache::new(12960, 1e6 as i64, tokio::spawn)?;

        Ok(SnapshotReader {
            ebs_client,
            snapshot,
            block_map: RefCell::new(block_map),
            internal_offset: 0,
            cur_offset: 0,
            max_offset: None,
            cache
        })
    }

    fn clone(&self) -> SnapshotReader {
        self.with_offset(0, None)
    }

    fn with_offset(&self, offset: u64, end: Option<u64>) -> SnapshotReader {
        SnapshotReader {
            ebs_client: self.ebs_client.clone(),
            snapshot: self.snapshot.clone(),
            block_map: self.block_map.clone(),
            internal_offset: self.internal_offset + offset,
            cur_offset: 0,
            max_offset: end,
            cache: self.cache.clone(),
        }
    }

    async fn get_block_token(&self, block_index: i32) -> Result<String> {
        if let Some(token) = self.block_map.borrow().block_map.get(&block_index) {
            return Ok(token.clone())
        }

        let mut found_block : Option<String> = None;
        let snapshot_id = self.snapshot.snapshot_id.as_str();

        let mut block_map = self.block_map.borrow_mut();

        while !block_map.block_map_complete {
            let response = self
                .ebs_client
                .list_snapshot_blocks()
                .snapshot_id(snapshot_id)
                .set_next_token(block_map.next_token.clone())
                .max_results(crate::download::LIST_REQUEST_MAX_RESULTS)
                .send()
                .await
                .context(error::ListSnapshotBlocksSnafu { snapshot_id })?;

            let blocks = response.blocks.unwrap_or_default();
            eprintln!("ListSnapshotBlocks {} - {}", blocks[0].block_index.unwrap(), blocks.last().unwrap().block_index.unwrap());

            for block in blocks.iter() {
                let index = block
                    .block_index
                    .context(error::FindBlockIndexSnafu { snapshot_id })?;

                let token = String::from(block.block_token.as_ref().context(
                    error::FindBlockPropertySnafu {
                        snapshot_id,
                        block_index: index,
                        property: "token",
                    },
                )?);

                block_map.block_map.insert(index, token.clone());

                if index == block_index {
                    found_block = Some(token);
                }
            }

            block_map.next_token = response.next_token;
            if block_map.next_token.is_none() {
                block_map.block_map_complete = true
            }
            if found_block.is_some() {
                break;
            }
        }

        Ok(found_block.context(error::FindBlockSnafu { snapshot_id, block_index })?)
    }

    async fn read_at_async(&self, pos: u64, buf: &mut [u8]) -> Result<usize> {
        let buf_size = buf.len();
        let mut end_byte = (pos as usize) + buf_size;
        if let Some(end) = self.max_offset {
            if pos >= end {
                return Err(Error::from(error::Error::ReadOutOfBounds { offset: pos }))
            }
            if end_byte > end as usize {
                end_byte = end as usize;
            }
        }

        let block_size = self.snapshot.block_size as usize;
        let start_block: i32 = ((pos as u64) / (block_size as u64)) as i32;
        let start_block_offset: usize = ((pos as u64) % (block_size as u64)) as usize;
        let end_block_offset: usize = ((end_byte as u64) % (block_size as u64)) as usize;
        let end_block: i32 = ((end_byte as u64) / (block_size as u64)) as i32; // off by 1?
        let block_count = (end_block - start_block) as usize;



        let mut block_contexts = Vec::with_capacity(block_count);
        let mut buf_offset = 0;
        let mut block_index = start_block;
        let mut existing: &mut [u8];
        let mut remaining: &mut [u8] = buf;

        loop {
            (existing, remaining) = remaining.split_at_mut(buf_offset);

            let mut size : usize = block_size;
            if block_index == end_block {
                size = end_block_offset;
            }
            if block_index == start_block {
                size -= start_block_offset;
            }

            let block_output : &mut [u8];
            (block_output, remaining) = remaining.split_at_mut(buf_offset + size);


            let block_token = self.get_block_token(block_index).await?;

            block_contexts.push(BlockContext {
                output_buffer: block_output,
                block_index,
                block_token,
                block_size: block_size as i32,
                snapshot_id: self.snapshot.snapshot_id.clone(),
                ebs_client: self.ebs_client.clone(),
                cache: &self.cache,
            });

            block_index += 1;
            buf_offset += size;
            if buf_offset >= buf_size {
                break;
            }
        }

        let stream = stream::iter(block_contexts).map(|ctx| -> Result<BlockContext> { Ok(ctx) });
        let download = stream.try_for_each_concurrent(
            SNAPSHOT_BLOCK_WORKERS,
            |context| async move {
                let mut block_data = download_block(&context).await?.clone();

                if context.block_index == start_block {
                   block_data.advance(start_block_offset);
                }
                if context.block_index == end_block {
                   block_data.truncate(end_block_offset);
                }
                block_data.copy_to_slice(context.output_buffer);

                Ok(())
            },
        );
        download.await?;

        Ok(buf.len())
    }
}

impl std::fmt::Debug for SnapshotReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SnapshotReader")
         .field(&self.snapshot)
         .field(&self.cur_offset)
         .finish()
    }
}

impl positioned_io2::ReadAt for SnapshotReader {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        let f = self.read_at_async(self.internal_offset + pos, buf);
        futures::executor::block_on(f).or_else(|err| -> std::result::Result<usize, std::io::Error> {
            Err(std::io::Error::new(ErrorKind::Other, err.to_string()))
        })
    }
}

impl std::io::Read for SnapshotReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.read_at(self.cur_offset + self.internal_offset, buf)?;
        self.cur_offset += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl std::io::Seek for SnapshotReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(i) => { self.cur_offset = i as u64 }
            SeekFrom::End(i) => { self.cur_offset = (self.snapshot.volume_size * GIBIBYTE) as u64 - i as u64 }
            SeekFrom::Current(i) => { self.cur_offset = ((self.cur_offset as i64) + i) as u64 }
        }
        Ok(self.cur_offset)
    }
}

impl std::io::Write for SnapshotReader {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        todo!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

/// Potential errors while downloading a snapshot and writing to a local file.
mod error {
    use aws_sdk_ebs::{
        self,
        operation::get_snapshot_block::GetSnapshotBlockError,
        operation::list_snapshot_blocks::ListSnapshotBlocksError,
    };
    use snafu::Snafu;
    use std::path::PathBuf;
    use gpt::GptError;
    use tokio::time::error::Elapsed;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(super)))]
    pub(super) enum Error {
        #[snafu(display("Failed to read metadata for '{}': {}", path.display(), source))]
        ReadFileMetadata {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("{}", source))]
        StdIoError { source: std::io::Error },

        #[snafu(display("{}", source))]
        CacheError { source: stretto::CacheError },

        #[snafu(display("{}", source))]
        UuidError { source: uuid::Error },

        #[snafu(display("{}", source))]
        GptError { source: GptError },

        #[snafu(display("{}", source))]
        TimeoutError { source: Elapsed },

        #[snafu(display("{}", source))]
        EyreError { source: color_eyre::eyre::Error },

        #[snafu(display("{}", source))]
        DownloadError { source: super::super::download::Error },

        #[snafu(display("Failed to find Linux partition"))]
        PartitionError { },

        #[snafu(display(
        "Failed to get block {} for snapshot '{}': {}",
        block_index,
        snapshot_id,
        source
        ))]
        GetSnapshotBlock {
            snapshot_id: String,
            block_index: i64,
            source: aws_sdk_ebs::error::SdkError<GetSnapshotBlockError>,
        },

        #[snafu(display("Failed to list snapshot blocks '{}': {}", snapshot_id, source))]
        ListSnapshotBlocks {
            snapshot_id: String,
            source: aws_sdk_ebs::error::SdkError<ListSnapshotBlocksError>,
        },

        #[snafu(display("Failed to find index for block in '{}'", snapshot_id))]
        FindBlockIndex { snapshot_id: String },

        #[snafu(display(
            "Failed to block {} for snapshot '{}'",
            block_index,
            snapshot_id
        ))]
        FindBlock {
            snapshot_id: String,
            block_index: i32,
        },

        #[snafu(display(
            "Failed to find {} for block {} in '{}'",
            property,
            block_index,
            snapshot_id
        ))]
        FindBlockProperty {
            snapshot_id: String,
            block_index: i32,
            property: String,
        },

        #[snafu(display(
            "Failed to find {} for block {} in '{}'",
            property,
            block_index,
            snapshot_id
        ))]
        CollectByteStream {
            snapshot_id: String,
            block_index: i32,
            property: String,
            source: aws_smithy_http::byte_stream::error::Error,
        },

        #[snafu(display("Read past end of exposed slice 0x{:x}", offset))]
        ReadOutOfBounds { offset: u64 },

        #[snafu(display("Failed to find block size for '{}'", snapshot_id))]
        FindBlockSize { snapshot_id: String },

        #[snafu(display(
            "Found unexpected checksum algorithm '{}' for block {} in '{}'",
            checksum_algorithm,
            block_index,
            snapshot_id
        ))]
        UnexpectedBlockChecksumAlgorithm {
            snapshot_id: String,
            block_index: i64,
            checksum_algorithm: String,
        },

        #[snafu(display(
            "Found unexpected data length {} for block {} in '{}'",
            data_length,
            block_index,
            snapshot_id
        ))]
        UnexpectedBlockDataLength {
            snapshot_id: String,
            block_index: i64,
            data_length: i64,
        },

        #[snafu(display(
            "Bad checksum for block {} in '{}': expected '{}', got '{}'",
            block_index,
            snapshot_id,
            expected_hash,
            block_hash,
        ))]
        BadBlockChecksum {
            snapshot_id: String,
            block_index: i64,
            block_hash: String,
            expected_hash: String,
        },

        #[snafu(display("Failed to convert {} {} to {}: {}", what, number, target, source))]
        ConvertNumber {
            what: String,
            number: String,
            target: String,
            source: std::num::TryFromIntError,
        },
    }
}
