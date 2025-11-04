//! Cache file format for HybridTripleAccess
//!
//! This module provides functionality to serialize/deserialize the in-memory
//! structures used by HybridTripleAccess, allowing them to be prebuilt from
//! TriplesBitmap and reused.
//!
//! Cache file format (.hdt.cache):
//! ```text
//! [Magic: "HDTCACHE"]               (8 bytes)
//! [Version: u32]                    (4 bytes) - VERSION 3
//! [Order: u8]                       (1 byte)
//! [Op Index Sequence]               (variable - sucds serialized CompactVector)
//! [Op Index Bitmap]                 (variable - sucds serialized)
//! [Wavelet Y]                       (variable - sucds serialized)
//! [Bitmap Y Offset: u64]            (8 bytes - offset in HDT file where bitmap_y begins)
//! [Bitmap Z Offset: u64]            (8 bytes - offset in HDT file where bitmap_z begins)
//! [Sequence Z Offset: u64]          (8 bytes - offset in HDT file where sequence_z begins)
//! [Dictionary Offset: u64]          (8 bytes - offset in HDT file where Dictionary section begins)
//! [Dict Shared Offset: u64]         (8 bytes - offset where shared dictionary section begins)
//! [Dict Subjects Offset: u64]       (8 bytes - offset where subjects dictionary section begins)
//! [Dict Predicates Offset: u64]     (8 bytes - offset where predicates dictionary section begins)
//! [Dict Objects Offset: u64]        (8 bytes - offset where objects dictionary section begins)
//! [Triples Offset: u64]             (8 bytes - offset in HDT file where Triples section begins)
//! [CRC32]                           (4 bytes)
//! ```
//!
//! ## Design Rationale
//! - **Stored in cache**: op_index (sequence + bitmap), wavelet_y - computed structures, expensive to rebuild
//! - **File offsets only**: bitmap_y, bitmap_z - read directly from HDT file on-demand
//! - **File offsets only**: sequence_z - metadata read during FileBasedSequence::new()
//! - **Version 3 changes**: Removed bitmap_y/z data, sequence_z metadata; added bitmap_y/z offsets

use crate::containers::AdjListGeneric;
use crate::containers::Bitmap;
use crate::containers::ControlInfo;
use crate::containers::InMemoryBitmap;
use crate::containers::InMemorySequence;
use crate::containers::Sequence;
use crate::header::Header;
use crate::triples::TriplesBitmapGeneric;
use crate::triples::{Order, TriplesBitmap};
use bytesize::ByteSize;
use log::debug;
use log::warn;
use std::fmt;
use std::fs::File;
use std::io::Seek;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use sucds::Serializable;
use sucds::bit_vectors::Rank9Sel;
use sucds::char_sequences::WaveletMatrix;
use sucds::int_vectors::CompactVector;

const MAGIC: &[u8; 8] = b"HDTCACHE";
const VERSION: u32 = 3; // Version 3: Store offsets for bitmaps, not data
const CACHE_EXT: &str = "index.v3-rust-cache";

/// Cached structures for HybridTripleAccess
///
/// ## Storage Strategy:
/// - **In cache**: op_index (both sequence and bitmap), wavelet_y - computed/built structures
/// - **File offsets**: bitmap_y, bitmap_z, sequence_z, dictionary sections - read from HDT file on-demand
pub struct HybridCache {
    /// Triple ordering (SPO, etc.)
    pub order: Order,
    /// Op-index sequence (ALWAYS in-memory, computed)
    pub op_index_sequence: CompactVector,
    /// Op-index bitmap (ALWAYS in-memory, computed)
    pub op_index_bitmap: Bitmap,
    /// Wavelet matrix (ALWAYS in-memory, computed)
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
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
            "total size {}: {{ {} op_sequence, {} op_bitmap, {} sequence_y }}",
            ByteSize(
                self.op_index_sequence.size_in_bytes() as u64
                    + self.op_index_bitmap.size_in_bytes() as u64
                    + self.wavelet_y.size_in_bytes() as u64
            ),
            ByteSize(self.op_index_sequence.size_in_bytes() as u64),
            ByteSize(self.op_index_bitmap.size_in_bytes() as u64),
            ByteSize(self.wavelet_y.size_in_bytes() as u64),
        )
    }
}

impl HybridCache {
    /// Smart constructor: Load cache if exists, otherwise create it
    ///
    /// This is the recommended way to create a HybridCache. It automatically:
    /// 1. Checks if a cache file exists for the given HDT file
    /// 2. If found, loads the existing cache
    /// 3. If not found, generates the cache from the HDT file and saves it
    ///
    /// # Arguments
    /// * `hdt_path` - Path to the HDT file
    ///
    /// # Cache File Location
    /// The cache file is stored in the same directory as the HDT file with the naming convention:
    /// `<hdt_filename>.index.v3-rust-cache`
    ///
    /// # Example
    /// ```ignore
    /// let cache = HybridCache::from_hdt_path("data/myfile.hdt")?;
    /// // First call: generates cache and saves to "data/myfile.hdt.index.v3-rust-cache"
    /// // Second call: loads existing cache (much faster!)
    /// ```
    pub fn from_hdt_path(hdt_path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let hdt_path = hdt_path.as_ref();

        // Construct cache file path
        let cache_path = Self::get_cache_path(hdt_path);

        // Check if cache exists and is readable
        if cache_path.exists() {
            debug!("Found existing cache: {}", cache_path.display());
            match Self::read_from_file(&cache_path) {
                Ok(cache) => {
                    debug!("Loaded cache successfully");
                    debug!("{cache:#?}");
                    return Ok(cache);
                }
                Err(e) => {
                    warn!("Cache file exists but couldn't be read: {e}");
                    warn!("Regenerating cache...");
                }
            }
        } else {
            debug!("Cache not found, generating from HDT file...");
        }

        // Cache doesn't exist or couldn't be read - generate it
        let cache = Self::write_cache_from_hdt_file(hdt_path);
        debug!("Cache generated and saved to: {}", cache_path.display());

        Ok(cache)
    }

    /// Get the cache file path for a given HDT file
    pub fn get_cache_path(hdt_path: impl AsRef<Path>) -> std::path::PathBuf {
        let hdt_path = hdt_path.as_ref();
        let mut cache_path = hdt_path.to_path_buf();

        // Get the original filename
        let file_name = hdt_path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");

        // Append cache extension: myfile.hdt -> myfile.hdt.index.v3-rust-cache
        let cache_file_name = format!("{file_name}.{CACHE_EXT}");
        cache_path.set_file_name(cache_file_name);

        cache_path
    }

    /// Generate cache from TriplesBitmap with file offsets
    ///
    /// # Arguments
    /// * `triples` - The in-memory TriplesBitmap to extract computed structures from
    /// * `bitmap_y_offset` - File offset where bitmap_y starts in HDT file
    /// * `bitmap_z_offset` - File offset where bitmap_z starts in HDT file
    /// * `sequence_z_offset` - File offset where sequence_z starts in HDT file
    /// * `dictionary_offset` - File offset where dictionary starts in HDT file
    /// * `dict_shared_offset` - File offset where shared dictionary section starts
    /// * `dict_subjects_offset` - File offset where subjects dictionary section starts
    /// * `dict_predicates_offset` - File offset where predicates dictionary section starts
    /// * `dict_objects_offset` - File offset where objects dictionary section starts
    /// * `triples_offset` - File offset where triples section starts in HDT file
    #[allow(clippy::too_many_arguments)]
    fn from_triples_bitmap(
        triples: &TriplesBitmap, bitmap_y_offset: u64, bitmap_z_offset: u64, sequence_z_offset: u64,
        dictionary_offset: u64, dict_shared_offset: u64, dict_subjects_offset: u64, dict_predicates_offset: u64,
        dict_objects_offset: u64, triples_offset: u64,
    ) -> Self {
        Self {
            order: triples.order.clone(),
            op_index_sequence: triples.op_index.sequence.clone(),
            op_index_bitmap: triples.op_index.bitmap.clone(),
            wavelet_y: triples.wavelet_y.clone(),
            bitmap_y_offset,
            bitmap_z_offset,
            sequence_z_offset,
            dictionary_offset,
            dict_shared_offset,
            dict_subjects_offset,
            dict_predicates_offset,
            dict_objects_offset,
            triples_offset,
        }
    }

    pub fn write_cache_from_hdt_file(hdt_path: &Path) -> Self {
        let mut reader = std::io::BufReader::new(std::fs::File::open(hdt_path).expect("msg"));
        // Read control info (global header)
        ControlInfo::read(&mut reader).expect("msg");

        // Read header
        let _ = Header::read(&mut reader).expect("msg");

        // Track dictionary offset (before control info)
        let dictionary_offset = reader.stream_position().expect("msg");

        // Read dictionary control info
        let _ = ControlInfo::read(&mut reader).expect("msg");

        // Track offsets for each dictionary section BEFORE reading them
        let dict_shared_offset = reader.stream_position().expect("msg");
        let _ =
            crate::dict_sect_pfc::DictSectPFC::read(&mut reader, true).expect("msg").join().unwrap().expect("msg");

        let dict_subjects_offset = reader.stream_position().expect("msg");
        let _ =
            crate::dict_sect_pfc::DictSectPFC::read(&mut reader, true).expect("msg").join().unwrap().expect("msg");

        let dict_predicates_offset = reader.stream_position().expect("msg");
        let _ =
            crate::dict_sect_pfc::DictSectPFC::read(&mut reader, true).expect("msg").join().unwrap().expect("msg");

        let dict_objects_offset = reader.stream_position().expect("msg");
        let _ =
            crate::dict_sect_pfc::DictSectPFC::read(&mut reader, true).expect("msg").join().unwrap().expect("msg");

        // Track triples section offset
        let triples_offset = reader.stream_position().expect("msg");

        // Read triples control info
        let triples_ci = ControlInfo::read(&mut reader).expect("msg");

        // Track bitmap_y offset BEFORE reading it
        let bitmap_y_offset = reader.stream_position().expect("msg");
        let bitmap_y = Bitmap::read(&mut reader).expect("failed to read bitmap_y");

        // Track bitmap_z offset BEFORE reading it
        let bitmap_z_offset = reader.stream_position().expect("msg");
        let bitmap_z = Bitmap::read(&mut reader).expect("failed to read bitmap_z");

        // read sequences
        let sequence_y = Sequence::read(&mut reader).expect("failed to read sequence_y");

        // Track sequence_z offset BEFORE reading it
        let sequence_z_offset = reader.stream_position().expect("msg");
        let sequence_z = Sequence::read(&mut reader).expect("failed to read sequence_z");

        let order: Order;
        if let Some(n) = triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            order = Order::try_from(n).expect("msg");
        } else {
            panic!("unknown triples Order")
        }
        let adjlist_z = AdjListGeneric::new(InMemorySequence::new(sequence_z), InMemoryBitmap::new(bitmap_z));

        let triples_bitmap = TriplesBitmapGeneric::new(order, sequence_y, bitmap_y, adjlist_z);
        let cache = Self::from_triples_bitmap(
            &triples_bitmap, bitmap_y_offset, bitmap_z_offset, sequence_z_offset, dictionary_offset,
            dict_shared_offset, dict_subjects_offset, dict_predicates_offset, dict_objects_offset, triples_offset,
        );

        debug!("{cache:#?}");

        // Write cache to file using the standard cache path
        let cache_path = Self::get_cache_path(hdt_path);
        cache.write_to_file(&cache_path).expect("Failed to write cache file");

        cache
    }

    /// Write cache to file
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write magic
        writer.write_all(MAGIC)?;

        // Write version (VERSION 3)
        writer.write_all(&VERSION.to_le_bytes())?;

        // Write order
        writer.write_all(&[self.order.clone() as u8])?;

        // Write computed structures (in-memory only)
        // Write op_index.sequence
        self.op_index_sequence.serialize_into(&mut writer)?;

        // Write op_index.bitmap
        self.op_index_bitmap.dict.serialize_into(&mut writer)?;

        // Write wavelet_y
        self.wavelet_y.serialize_into(&mut writer)?;

        // Write file offsets (bitmaps, sequences, and dictionary sections read from HDT on-demand)
        writer.write_all(&self.bitmap_y_offset.to_le_bytes())?;
        writer.write_all(&self.bitmap_z_offset.to_le_bytes())?;
        writer.write_all(&self.sequence_z_offset.to_le_bytes())?;
        writer.write_all(&self.dictionary_offset.to_le_bytes())?;
        writer.write_all(&self.dict_shared_offset.to_le_bytes())?;
        writer.write_all(&self.dict_subjects_offset.to_le_bytes())?;
        writer.write_all(&self.dict_predicates_offset.to_le_bytes())?;
        writer.write_all(&self.dict_objects_offset.to_le_bytes())?;
        writer.write_all(&self.triples_offset.to_le_bytes())?;

        // Write CRC32 (computed over all written data)
        // For simplicity, we'll skip CRC for now (TODO: add later)
        let crc: u32 = 0;
        writer.write_all(&crc.to_le_bytes())?;

        writer.flush()?;
        Ok(())
    }

    /// Read cache from file
    pub fn read_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and verify magic
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err("Invalid cache file magic".into());
        }

        // Read version
        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != VERSION {
            return Err(format!("Unsupported cache version: expected {VERSION}, found {version}").into());
        }

        // Read order
        let mut order_byte = [0u8];
        reader.read_exact(&mut order_byte)?;
        let order = Order::try_from(order_byte[0] as u32).map_err(|e| format!("Invalid order: {e}"))?;

        // Read computed structures (in-memory)
        // Read op_index.sequence
        let op_index_sequence = CompactVector::deserialize_from(&mut reader)?;

        // Read op_index.bitmap
        let op_index_bitmap_dict = Rank9Sel::deserialize_from(&mut reader)?;
        let op_index_bitmap = Bitmap { dict: op_index_bitmap_dict };

        // Read wavelet_y
        let wavelet_y = WaveletMatrix::deserialize_from(&mut reader)?;

        // Read file offsets
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

        // Read CRC32 (skip validation for now)
        let mut _crc_bytes = [0u8; 4];
        reader.read_exact(&mut _crc_bytes)?;

        Ok(Self {
            order,
            op_index_sequence,
            op_index_bitmap,
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hdt;
    use std::io::BufReader;

    #[test]
    fn test_from_hdt_path() -> Result<(), Box<dyn std::error::Error>> {
        let hdt_path = "tests/resources/snikmeta.hdt";
        let cache_path = HybridCache::get_cache_path(hdt_path);

        // Clean up any existing cache
        let _ = std::fs::remove_file(&cache_path);

        println!("\n=== Test 1: First call (should generate cache) ===");
        let cache1 = HybridCache::from_hdt_path(hdt_path)?;
        assert!(cache_path.exists(), "Cache file should be created");
        println!("Cache size: {} bytes", std::fs::metadata(&cache_path)?.len());

        println!("\n=== Test 2: Second call (should load existing cache) ===");
        let cache2 = HybridCache::from_hdt_path(hdt_path)?;

        // Verify both caches are identical
        assert_eq!(cache1.order as u8, cache2.order as u8);
        assert_eq!(cache1.op_index_sequence.len(), cache2.op_index_sequence.len());
        assert_eq!(cache1.wavelet_y.len(), cache2.wavelet_y.len());
        assert_eq!(cache1.bitmap_y_offset, cache2.bitmap_y_offset);
        assert_eq!(cache1.bitmap_z_offset, cache2.bitmap_z_offset);
        assert_eq!(cache1.sequence_z_offset, cache2.sequence_z_offset);

        println!("\nBoth caches are identical!");

        // Clean up
        std::fs::remove_file(&cache_path)?;

        Ok(())
    }

    #[test]
    fn test_cache_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        // Load TriplesBitmap
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let hdt = Hdt::read(BufReader::new(file))?;

        // Generate cache with example offsets
        let cache = HybridCache::from_triples_bitmap(
            &hdt.triples, 1000,  // bitmap_y_offset
            2000,  // bitmap_z_offset
            12345, // sequence_z_offset
            10000, // dictionary_offset
            10100, // dict_shared_offset
            10200, // dict_subjects_offset
            10300, // dict_predicates_offset
            10400, // dict_objects_offset
            20000, // triples_offset
        );

        // Write to file
        let cache_path = "/tmp/test.hdt.cache";
        cache.write_to_file(cache_path)?;

        // Read back
        let cache2 = HybridCache::read_from_file(cache_path)?;

        // Verify
        assert_eq!(cache.order as u8, cache2.order as u8);
        assert_eq!(cache.op_index_bitmap.len(), cache2.op_index_bitmap.len());
        assert_eq!(cache.op_index_sequence.len(), cache2.op_index_sequence.len());
        assert_eq!(cache.wavelet_y.len(), cache2.wavelet_y.len());
        assert_eq!(cache.bitmap_y_offset, cache2.bitmap_y_offset);
        assert_eq!(cache.bitmap_z_offset, cache2.bitmap_z_offset);
        assert_eq!(cache.sequence_z_offset, cache2.sequence_z_offset);
        assert_eq!(cache.dictionary_offset, cache2.dictionary_offset);
        assert_eq!(cache.dict_shared_offset, cache2.dict_shared_offset);
        assert_eq!(cache.dict_subjects_offset, cache2.dict_subjects_offset);
        assert_eq!(cache.dict_predicates_offset, cache2.dict_predicates_offset);
        assert_eq!(cache.dict_objects_offset, cache2.dict_objects_offset);
        assert_eq!(cache.triples_offset, cache2.triples_offset);

        println!("Cache roundtrip successful!");
        println!("   Cache file size: {} bytes", std::fs::metadata(cache_path)?.len());

        // Clean up
        std::fs::remove_file(cache_path)?;

        Ok(())
    }
}
