//! Cache file format for HybridTripleAccess
//!
//! This module provides functionality to serialize/deserialize the in-memory
//! structures used by HybridTripleAccess, allowing them to be prebuilt from
//! TriplesBitmap and reused.
//!
//! Cache file format (.hdt.index.v5-rust-cache):
//! ```text
//! [ControlInfo]                     (HDT ControlInfo structure with type=Index)
//!   - format: "<http://purl.org/HDT/hdt#cacheV5>"
//!   - properties["order"]           (SPO=1, SOP=2, PSO=3, etc.)
//!   - properties["numTriples"]      (total number of triples)
//!   - properties["headerSize"]      (size of HDT header section in bytes)
//! [Wavelet Y]                       (variable - bincode serialized QWaveletTree)
//! [Bitmap Y Offset: u64]            (8 bytes - offset in HDT file where bitmap_y begins)
//! [Bitmap Z Offset: u64]            (8 bytes - offset in HDT file where bitmap_z begins)
//! [Sequence Z Offset: u64]          (8 bytes - offset in HDT file where sequence_z begins)
//! [Dictionary Offset: u64]          (8 bytes - offset in HDT file where Dictionary section begins)
//! [Dict Shared Offset: u64]         (8 bytes - offset where shared dictionary section begins)
//! [Dict Subjects Offset: u64]       (8 bytes - offset where subjects dictionary section begins)
//! [Dict Predicates Offset: u64]     (8 bytes - offset where predicates dictionary section begins)
//! [Dict Objects Offset: u64]        (8 bytes - offset where objects dictionary section begins)
//! [Triples Offset: u64]             (8 bytes - offset in HDT file where Triples section begins)
//! [Op Index Bitmap]                 (variable - serialized Bitmap, offset returned by read_from_file())
//! [Op Index Sequence]               (variable - serialized Sequence)
//! ```
//!
//! ## Design Rationale
//! - **Stored in cache (in memory)**: wavelet_y - computed structure, expensive to rebuild, always loaded
//! - **Stored in cache (on disk)**: op_index.bitmap, op_index.sequence - can be accessed on-demand or mmapped
//! - **File offsets only**: bitmap_y, bitmap_z - read directly from HDT file on-demand
//! - **File offsets only**: sequence_z - metadata parsed during MmapSequence::from_mmap()
//! - **Version 5 changes**: Use ControlInfo structure, moved order/numTriples/headerSize to properties

use crate::containers::AdjList;
use crate::containers::Bitmap;
use crate::containers::ControlInfo;
use crate::containers::Sequence;
use crate::header::Header;
use crate::triples::Order;
use crate::triples::TriplesBitmap;
use crate::triples::WT;
use bytesize::ByteSize;
use fs2::FileExt;
use log::debug;
use log::warn;
use mem_dbg::{MemSize, SizeFlags};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::Seek;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub const CACHE_EXT: &str = "index.v5-rust-cache";
const CACHE_FORMAT: &str = "<http://purl.org/HDT/hdt#cacheV5>";

/// Typed error for everything that can go wrong when loading, validating,
/// or generating a `HybridCache`. Replaces the previous `Box<dyn Error>`
/// surface so callers (notably `Hdt::new_hybrid_cache`) get structured
/// errors with intact source chains.
#[derive(thiserror::Error, Debug)]
pub enum HybridCacheError {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("failed to read HDT control info")]
    ControlInfo(#[from] crate::containers::control_info::Error),
    #[error("failed to read HDT header")]
    Header(#[from] crate::header::Error),
    #[error("failed to read HDT bitmap")]
    Bitmap(#[from] crate::containers::bitmap::Error),
    #[error("failed to read HDT sequence")]
    Sequence(#[from] crate::containers::sequence::Error),
    #[error("failed to read HDT dictionary section")]
    DictSect(#[from] crate::dict_sect_pfc::Error),
    #[error("failed to construct HDT triples section")]
    Triples(#[from] crate::triples::Error),
    #[error("bincode decode error")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("bincode encode error")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("invalid cache control type: expected Index, found {found:?}")]
    InvalidControlType { found: crate::containers::ControlType },
    #[error("unsupported cache format: expected {expected}, found {found}")]
    UnsupportedFormat { expected: String, found: String },
    #[error("missing required cache property: {0}")]
    MissingProperty(&'static str),
    #[error("invalid value for cache property {key}: {value}")]
    InvalidProperty { key: &'static str, value: String },
    #[error("HDT file has no triples; hybrid cache cannot be created")]
    EmptyHdt,
    #[error("DictSectPFC read worker thread panicked")]
    DictSectThreadPanic,
    #[error("failed to canonicalize HDT path {path}")]
    Canonicalize {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to {op} cache lock file {path}")]
    Lock {
        op: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to atomically rename cache temp {tmp} to {final_path}")]
    Rename {
        tmp: String,
        final_path: String,
        #[source]
        source: std::io::Error,
    },
}

pub type HybridCacheResult<T> = core::result::Result<T, HybridCacheError>;

/// Canonicalize the HDT path so that two callers that reach the same file
/// via different relative paths still hash to the same lock file.
fn canonical_hdt_path(path: &Path) -> HybridCacheResult<PathBuf> {
    path.canonicalize().map_err(|source| HybridCacheError::Canonicalize {
        path: path.display().to_string(),
        source,
    })
}

/// Build the path of the lock file used to serialize cache generation for
/// the given canonical HDT path. Lives in the system temp directory under
/// `hdt-hybrid-cache-locks/`, keyed by the FNV-style hash of the path.
fn cache_lock_file_path(canonical_hdt_path: &Path) -> HybridCacheResult<PathBuf> {
    let mut hasher = DefaultHasher::new();
    canonical_hdt_path.as_os_str().hash(&mut hasher);
    let lock_name = format!("hdt-hybrid-cache-{:016x}.lock", hasher.finish());
    let lock_root = std::env::temp_dir().join("hdt-hybrid-cache-locks");
    std::fs::create_dir_all(&lock_root).map_err(|source| HybridCacheError::Lock {
        op: "create directory for",
        path: lock_root.display().to_string(),
        source,
    })?;
    Ok(lock_root.join(lock_name))
}

fn open_cache_lock_file(canonical_hdt_path: &Path) -> HybridCacheResult<(File, PathBuf)> {
    let lock_path = cache_lock_file_path(canonical_hdt_path)?;
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| HybridCacheError::Lock {
            op: "open",
            path: lock_path.display().to_string(),
            source,
        })?;
    Ok((lock_file, lock_path))
}

fn unlock_cache_lock(lock_file: &File, lock_path: &Path, mode: &'static str) -> HybridCacheResult<()> {
    FileExt::unlock(lock_file).map_err(|source| HybridCacheError::Lock {
        op: match mode {
            "shared" => "release shared",
            "exclusive" => "release exclusive",
            _ => "release",
        },
        path: lock_path.display().to_string(),
        source,
    })
}

/// Cached structures for HybridTripleAccess
///
/// ## Storage Strategy:
/// - **In cache (in memory)**: wavelet_y - computed/built structures loaded into memory
/// - **In cache (on disk)**: op_index.bitmap and op_index.sequence - written at end of cache file, offsets returned by read_from_file()
/// - **HDT file offsets**: bitmap_y, bitmap_z, sequence_z, dictionary sections - read from HDT file on-demand
/// - **Metadata in ControlInfo**: order, numTriples, headerSize stored in properties
pub struct HybridCache {
    /// Control information containing metadata (order, numTriples, headerSize)
    pub control_info: ControlInfo,
    /// Wavelet tree (stored in cache file, always loaded into memory)
    pub wavelet_y: WT,
    /// File offset where bitmap_y begins in HDT file
    pub bitmap_y_offset: u64,
    /// File offset where bitmap_z (adjlist_z.bitmap) begins in HDT file
    pub bitmap_z_offset: u64,
    /// File offset where sequence_z (adjlist_z.sequence) begins in HDT file
    pub sequence_z_offset: u64,
    /// File offset where Dictionary section begins in HDT file
    pub dictionary_offset: u64,
    /// File offset where shared dictionary section begins
    pub dict_shared_offset: u64,
    /// File offset where subjects dictionary section begins
    pub dict_subjects_offset: u64,
    /// File offset where predicates dictionary section begins
    pub dict_predicates_offset: u64,
    /// File offset where objects dictionary section begins
    pub dict_objects_offset: u64,
    /// File offset where Triples section begins in HDT file
    pub triples_offset: u64,
}

impl fmt::Debug for HybridCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "in-memory size {}: {{ {} wavelet_y }}",
            ByteSize(self.wavelet_y.mem_size(SizeFlags::default()) as u64),
            ByteSize(self.wavelet_y.mem_size(SizeFlags::default()) as u64),
        )
    }
}

impl HybridCache {
    /// Get the triple ordering from cache metadata
    pub fn order(&self) -> HybridCacheResult<Order> {
        let value = self
            .control_info
            .get("order")
            .ok_or(HybridCacheError::MissingProperty("order"))?;
        let parsed = value
            .parse::<u8>()
            .map_err(|_| HybridCacheError::InvalidProperty { key: "order", value: value.clone() })?;
        Order::try_from(parsed as u32).map_err(|_| HybridCacheError::InvalidProperty {
            key: "order",
            value,
        })
    }

    /// Get the number of triples from cache metadata
    pub fn num_triples(&self) -> HybridCacheResult<usize> {
        let value = self
            .control_info
            .get("numTriples")
            .ok_or(HybridCacheError::MissingProperty("numTriples"))?;
        value
            .parse::<usize>()
            .map_err(|_| HybridCacheError::InvalidProperty { key: "numTriples", value })
    }

    /// Get the header size from cache metadata
    pub fn header_size(&self) -> HybridCacheResult<u64> {
        let value = self
            .control_info
            .get("headerSize")
            .ok_or(HybridCacheError::MissingProperty("headerSize"))?;
        value
            .parse::<u64>()
            .map_err(|_| HybridCacheError::InvalidProperty { key: "headerSize", value })
    }
}

impl HybridCache {
    /// Smart constructor: Load cache if exists, otherwise create it.
    ///
    /// Uses a cross-process advisory file lock keyed on the canonical HDT
    /// path so that concurrent loaders never duplicate cache generation work
    /// and never observe a half-written cache file. The lock pattern is:
    ///
    /// 1. Acquire a shared lock and try to read an existing cache.
    /// 2. If the cache exists and parses, return it (shared lock allows
    ///    multiple concurrent readers).
    /// 3. Otherwise upgrade to an exclusive lock, re-check the cache, and
    ///    only generate if it is still missing or unreadable.
    /// 4. The writer always emits to a temporary file first and atomically
    ///    renames it into place, so readers either see the previous cache
    ///    or the next one — never a partial file.
    ///
    /// # Arguments
    /// * `hdt_path` - Path to the HDT file
    ///
    /// # Cache File Location
    /// `<hdt_filename>.index.v5-rust-cache`, alongside the HDT file.
    /// The lock file lives in `${TMPDIR}/hdt-hybrid-cache-locks/`.
    ///
    /// # Returns
    /// Returns a tuple `(HybridCache, u64)` where:
    /// - `HybridCache`: The loaded/created cache
    /// - `u64`: File offset in the cache file where the OpIndex bitmap begins
    pub fn from_hdt_path(hdt_path: impl AsRef<Path>) -> HybridCacheResult<(Self, u64)> {
        let hdt_path = hdt_path.as_ref();
        let canonical = canonical_hdt_path(hdt_path)?;
        let (lock_file, lock_path) = open_cache_lock_file(&canonical)?;

        let cache_path = Self::get_cache_path(hdt_path);

        // Reader path: shared lock allows many concurrent loaders to share an
        // already-built cache without blocking each other.
        FileExt::lock_shared(&lock_file).map_err(|source| HybridCacheError::Lock {
            op: "acquire shared",
            path: lock_path.display().to_string(),
            source,
        })?;

        if cache_path.exists() {
            debug!("Found existing cache: {}", cache_path.display());
            match Self::read_from_file(&cache_path) {
                Ok((cache, op_index_bitmap_offset)) => {
                    debug!("Loaded cache successfully");
                    debug!("{cache:#?}");
                    unlock_cache_lock(&lock_file, &lock_path, "shared")?;
                    return Ok((cache, op_index_bitmap_offset));
                }
                Err(e) => {
                    warn!("Cache file exists but couldn't be read: {e}");
                    warn!("Regenerating cache...");
                }
            }
        } else {
            debug!("Cache not found, generating from HDT file...");
        }

        unlock_cache_lock(&lock_file, &lock_path, "shared")?;

        // Writer path: serialize cache regeneration. We must release the
        // shared lock first because flock() upgrade is not portable.
        FileExt::lock_exclusive(&lock_file).map_err(|source| HybridCacheError::Lock {
            op: "acquire exclusive",
            path: lock_path.display().to_string(),
            source,
        })?;

        // Re-check after acquiring the exclusive lock — another process may
        // have generated the cache while we were waiting.
        if cache_path.exists() {
            debug!("Re-checking cache after acquiring exclusive lock");
            if let Ok((cache, op_index_bitmap_offset)) = Self::read_from_file(&cache_path) {
                unlock_cache_lock(&lock_file, &lock_path, "exclusive")?;
                return Ok((cache, op_index_bitmap_offset));
            }
            warn!("Cache remained unreadable under exclusive lock; regenerating...");
        }

        let generated = Self::write_cache_from_hdt_file(hdt_path);
        unlock_cache_lock(&lock_file, &lock_path, "exclusive")?;
        generated
    }

    /// Get the cache file path for a given HDT file
    pub fn get_cache_path(hdt_path: impl AsRef<Path>) -> std::path::PathBuf {
        let hdt_path = hdt_path.as_ref();
        let mut cache_path = hdt_path.to_path_buf();

        // Get the original filename
        let file_name = hdt_path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");

        // Append cache extension: myfile.hdt -> myfile.hdt.index.v5-rust-cache
        let cache_file_name = format!("{file_name}.{CACHE_EXT}");
        cache_path.set_file_name(cache_file_name);

        cache_path
    }

    pub fn write_cache_from_hdt_file(hdt_path: &Path) -> HybridCacheResult<(Self, u64)> {
        use crate::containers::ControlType;
        use std::collections::HashMap;
        use std::io::Seek;

        let mut reader = std::io::BufReader::new(std::fs::File::open(hdt_path)?);
        // Read control info (global header)
        ControlInfo::read(&mut reader)?;

        // Read header and get its size
        let header = Header::read(&mut reader)?;
        let header_size = header.length as u64;

        // Track dictionary offset (before control info)
        let dictionary_offset = reader.stream_position()?;

        // Read dictionary control info
        let _ = ControlInfo::read(&mut reader)?;

        // Track offsets for each dictionary section BEFORE reading them.
        // The .join().unwrap() on the parsing thread is documented at the
        // unwrap site: it can only panic if the worker thread panicked,
        // which is itself a bug we want to surface.
        let dict_shared_offset = reader.stream_position()?;
        crate::dict_sect_pfc::DictSectPFC::read(&mut reader)?
            .join()
            .map_err(|_| HybridCacheError::DictSectThreadPanic)??;

        let dict_subjects_offset = reader.stream_position()?;
        crate::dict_sect_pfc::DictSectPFC::read(&mut reader)?
            .join()
            .map_err(|_| HybridCacheError::DictSectThreadPanic)??;

        let dict_predicates_offset = reader.stream_position()?;
        crate::dict_sect_pfc::DictSectPFC::read(&mut reader)?
            .join()
            .map_err(|_| HybridCacheError::DictSectThreadPanic)??;

        let dict_objects_offset = reader.stream_position()?;
        crate::dict_sect_pfc::DictSectPFC::read(&mut reader)?
            .join()
            .map_err(|_| HybridCacheError::DictSectThreadPanic)??;

        // Track triples section offset
        let triples_offset = reader.stream_position()?;

        // Read triples control info
        let triples_ci = ControlInfo::read(&mut reader)?;

        // Track bitmap_y offset BEFORE reading it
        let bitmap_y_offset = reader.stream_position()?;
        let bitmap_y = Bitmap::read(&mut reader)?;

        // Track bitmap_z offset BEFORE reading it
        let bitmap_z_offset = reader.stream_position()?;
        let bitmap_z = Bitmap::read(&mut reader)?;

        // read sequences
        let sequence_y = Sequence::read(&mut reader)?;

        // Track sequence_z offset BEFORE reading it
        let sequence_z_offset = reader.stream_position()?;
        let sequence_z = Sequence::read(&mut reader)?;

        let order_value = triples_ci
            .get("order")
            .ok_or(HybridCacheError::MissingProperty("order"))?;
        let order_num = order_value
            .parse::<u32>()
            .map_err(|_| HybridCacheError::InvalidProperty { key: "order", value: order_value.clone() })?;
        let order = Order::try_from(order_num)
            .map_err(|_| HybridCacheError::InvalidProperty { key: "order", value: order_value })?;
        let adjlist_z = AdjList::new(sequence_z, bitmap_z);

        // QWT library panics on empty data, so skip cache for empty HDT files.
        if adjlist_z.is_empty() {
            return Err(HybridCacheError::EmptyHdt);
        }

        let triples_bitmap = TriplesBitmap::new(order, &sequence_y, bitmap_y, adjlist_z);

        // Prepare a temporary cache file in the same directory, then atomically
        // rename it onto the final cache path once writing is complete. This
        // ensures readers (which may have the cache mmaped) never see a partial
        // file: a fresh inode appears under the final name, leaving any stale
        // mapping pointing at the old inode until it is dropped.
        let cache_path = Self::get_cache_path(hdt_path);
        let file_name = cache_path.file_name().and_then(|n| n.to_str()).unwrap_or("hdt-cache");
        let tmp_cache_path = cache_path.with_file_name(format!(
            "{file_name}.tmp-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_cache_path)?;
        let mut writer = BufWriter::new(file);

        // Create ControlInfo with metadata in properties
        let mut properties = HashMap::new();
        properties.insert("order".to_owned(), (triples_bitmap.order.clone() as u8).to_string());
        properties.insert("numTriples".to_owned(), triples_bitmap.adjlist_z.len().to_string());
        properties.insert("headerSize".to_owned(), header_size.to_string());
        let control_info =
            ControlInfo { control_type: ControlType::Index, format: CACHE_FORMAT.to_owned(), properties };

        // Write ControlInfo
        control_info.write(&mut writer)?;

        // Write wavelet_y using bincode
        bincode::serde::encode_into_std_write(&triples_bitmap.wavelet_y, &mut writer, bincode::config::standard())?;

        // Write all HDT file offsets
        writer.write_all(&bitmap_y_offset.to_le_bytes())?;
        writer.write_all(&bitmap_z_offset.to_le_bytes())?;
        writer.write_all(&sequence_z_offset.to_le_bytes())?;
        writer.write_all(&dictionary_offset.to_le_bytes())?;
        writer.write_all(&dict_shared_offset.to_le_bytes())?;
        writer.write_all(&dict_subjects_offset.to_le_bytes())?;
        writer.write_all(&dict_predicates_offset.to_le_bytes())?;
        writer.write_all(&dict_objects_offset.to_le_bytes())?;
        writer.write_all(&triples_offset.to_le_bytes())?;

        let op_index_offset = writer.stream_position()?;

        // Write op_index.bitmap, then op_index.sequence at the END of the file
        // (offset returned by read_from_file(), both can be accessed on-demand)
        triples_bitmap.op_index.bitmap.inner().write(&mut writer)?;
        triples_bitmap.op_index.sequence.inner().write(&mut writer)?;

        writer.flush()?;
        // Make sure every byte hits the disk before we swing the rename.
        // Without sync_all() a crash between rename and journal commit could
        // leave the new inode visible but empty.
        let file = writer.into_inner().map_err(|e| HybridCacheError::Io(e.into_error()))?;
        file.sync_all()?;
        drop(file);

        // Atomic publish: rename the temp file onto the final cache path.
        // On Unix this is a single inode swap; on Windows, std uses MoveFileEx
        // with MOVEFILE_REPLACE_EXISTING semantics so it overwrites in place.
        std::fs::rename(&tmp_cache_path, &cache_path).map_err(|source| {
            // Best-effort cleanup of the orphaned temp file.
            let _ = std::fs::remove_file(&tmp_cache_path);
            HybridCacheError::Rename {
                tmp: tmp_cache_path.display().to_string(),
                final_path: cache_path.display().to_string(),
                source,
            }
        })?;

        // Create and return the cache structure
        let cache = Self {
            control_info,
            wavelet_y: triples_bitmap.wavelet_y.clone(),
            bitmap_y_offset,
            bitmap_z_offset,
            sequence_z_offset,
            dictionary_offset,
            dict_shared_offset,
            dict_subjects_offset,
            dict_predicates_offset,
            dict_objects_offset,
            triples_offset,
        };

        debug!("Cache generated and saved to: {}", cache_path.display());
        debug!("{cache:#?}");
        Ok((cache, op_index_offset))
    }

    /// Read cache from file, returning the cache structure and the offset to the OpIndex data
    ///
    /// # Returns
    /// Returns a tuple `(HybridCache, u64)` where:
    /// - `HybridCache`: The loaded cache with in-memory structures (wavelet_y only)
    /// - `u64`: File offset in the cache file where the OpIndex data begins (bitmap then sequence).
    ///   Callers can use this offset to construct both bitmap and sequence accessors.
    pub fn read_from_file<P: AsRef<Path>>(path: P) -> HybridCacheResult<(Self, u64)> {
        use crate::containers::ControlType;
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and verify ControlInfo
        let control_info = ControlInfo::read(&mut reader)?;

        // Verify it's an Index type
        if control_info.control_type != ControlType::Index {
            return Err(HybridCacheError::InvalidControlType { found: control_info.control_type });
        }

        // Verify format
        if control_info.format != CACHE_FORMAT {
            return Err(HybridCacheError::UnsupportedFormat {
                expected: CACHE_FORMAT.to_owned(),
                found: control_info.format,
            });
        }

        // Read wavelet_y using bincode
        let wavelet_y: WT = bincode::serde::decode_from_std_read(&mut reader, bincode::config::standard())?;

        // Read HDT file offsets
        let mut bitmap_y_offset_bytes = [0u8; 8];
        reader.read_exact(&mut bitmap_y_offset_bytes)?;
        let bitmap_y_offset = u64::from_le_bytes(bitmap_y_offset_bytes);

        let mut bitmap_z_offset_bytes = [0u8; 8];
        reader.read_exact(&mut bitmap_z_offset_bytes)?;
        let bitmap_z_offset = u64::from_le_bytes(bitmap_z_offset_bytes);

        let mut sequence_z_offset_bytes = [0u8; 8];
        reader.read_exact(&mut sequence_z_offset_bytes)?;
        let sequence_z_offset = u64::from_le_bytes(sequence_z_offset_bytes);

        let mut dictionary_offset_bytes = [0u8; 8];
        reader.read_exact(&mut dictionary_offset_bytes)?;
        let dictionary_offset = u64::from_le_bytes(dictionary_offset_bytes);

        let mut dict_shared_offset_bytes = [0u8; 8];
        reader.read_exact(&mut dict_shared_offset_bytes)?;
        let dict_shared_offset = u64::from_le_bytes(dict_shared_offset_bytes);

        let mut dict_subjects_offset_bytes = [0u8; 8];
        reader.read_exact(&mut dict_subjects_offset_bytes)?;
        let dict_subjects_offset = u64::from_le_bytes(dict_subjects_offset_bytes);

        let mut dict_predicates_offset_bytes = [0u8; 8];
        reader.read_exact(&mut dict_predicates_offset_bytes)?;
        let dict_predicates_offset = u64::from_le_bytes(dict_predicates_offset_bytes);

        let mut dict_objects_offset_bytes = [0u8; 8];
        reader.read_exact(&mut dict_objects_offset_bytes)?;
        let dict_objects_offset = u64::from_le_bytes(dict_objects_offset_bytes);

        let mut triples_offset_bytes = [0u8; 8];
        reader.read_exact(&mut triples_offset_bytes)?;
        let triples_offset = u64::from_le_bytes(triples_offset_bytes);

        // The OpIndex data (bitmap then sequence) starts right after all the offsets
        let op_index_offset = reader.stream_position()?;

        let cache = Self {
            control_info,
            wavelet_y,
            bitmap_y_offset,
            bitmap_z_offset,
            sequence_z_offset,
            dictionary_offset,
            dict_shared_offset,
            dict_subjects_offset,
            dict_predicates_offset,
            dict_objects_offset,
            triples_offset,
        };

        Ok((cache, op_index_offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Copy the snikmeta test HDT into a temp directory keyed on the test
    /// name and current pid+timestamp so that concurrent tests cannot
    /// collide on the same cache file.
    fn setup_isolated_hdt(test_name: &str) -> Result<(PathBuf, PathBuf, PathBuf), Box<dyn std::error::Error>> {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let test_dir = std::env::temp_dir()
            .join(format!("hdt-hybrid-cache-test-{test_name}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&test_dir)?;

        let hdt_path = test_dir.join("snikmeta.hdt");
        std::fs::copy("tests/resources/snikmeta.hdt", &hdt_path)?;
        let cache_path = HybridCache::get_cache_path(&hdt_path);
        Ok((test_dir, hdt_path, cache_path))
    }

    #[test]
    fn test_from_hdt_path() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::init();
        let (test_dir, hdt_path, cache_path) = setup_isolated_hdt("single")?;
        let _ = std::fs::remove_file(&cache_path);

        println!("\n=== Test 1: First call (should generate cache) ===");
        let (cache1, offset1) = HybridCache::from_hdt_path(&hdt_path)?;
        assert!(cache_path.exists(), "Cache file should be created");
        println!("Cache size: {} bytes", std::fs::metadata(&cache_path)?.len());

        println!("\n=== Test 2: Second call (should load existing cache) ===");
        let (cache2, offset2) = HybridCache::from_hdt_path(&hdt_path)?;

        // Verify both caches are identical
        assert_eq!(cache1.order()? as u8, cache2.order()? as u8);
        assert_eq!(cache1.wavelet_y.len(), cache2.wavelet_y.len());
        assert_eq!(cache1.bitmap_y_offset, cache2.bitmap_y_offset);
        assert_eq!(cache1.bitmap_z_offset, cache2.bitmap_z_offset);
        assert_eq!(cache1.sequence_z_offset, cache2.sequence_z_offset);
        assert_eq!(offset1, offset2, "OpIndex offsets should match");

        println!("\nBoth caches are identical!");

        std::fs::remove_dir_all(test_dir)?;
        Ok(())
    }

    /// Hammer `from_hdt_path` from many threads with no preexisting cache.
    /// All workers should converge on a single valid cache without panics,
    /// torn writes, or duplicate generation racing.
    #[test]
    fn test_from_hdt_path_parallel_threads() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::init();
        let (test_dir, hdt_path, cache_path) = setup_isolated_hdt("parallel")?;
        let _ = std::fs::remove_file(&cache_path);

        let workers = 8_usize;
        let barrier = Arc::new(Barrier::new(workers));
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let path = hdt_path.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<(), String> {
                barrier.wait();
                HybridCache::from_hdt_path(&path).map(|_| ()).map_err(|e| e.to_string())
            }));
        }

        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    return Err(std::io::Error::other(format!(
                        "hybrid cache worker failed while loading cache: {e}"
                    ))
                    .into());
                }
                Err(_) => {
                    return Err(std::io::Error::other("hybrid cache worker thread panicked").into());
                }
            }
        }

        assert!(cache_path.exists(), "cache should exist after concurrent loads");
        std::fs::remove_dir_all(test_dir)?;
        Ok(())
    }
}
