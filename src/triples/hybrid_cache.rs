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
use crate::four_sect_dict::FourSectDict;
use crate::header::Header;
use crate::triples::TriplesBitmapGeneric;
use crate::triples::{Order, TriplesBitmap};
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
/// - **File offsets**: bitmap_y, bitmap_z, sequence_z - read from HDT file on-demand
#[derive(Debug)]
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
    /// File offset where Triples section begins in HDT file
    pub triples_offset: u64,
}

impl HybridCache {
    /// Generate cache from TriplesBitmap with file offsets
    ///
    /// # Arguments
    /// * `triples` - The in-memory TriplesBitmap to extract computed structures from
    /// * `bitmap_y_offset` - File offset where bitmap_y starts in HDT file
    /// * `bitmap_z_offset` - File offset where bitmap_z starts in HDT file
    /// * `sequence_z_offset` - File offset where sequence_z starts in HDT file
    /// * `dictionary_offset` - File offset where dictionary starts in HDT file
    /// * `triples_offset` - File offset where triples section starts in HDT file
    fn from_triples_bitmap(
        triples: &TriplesBitmap,
        bitmap_y_offset: u64,
        bitmap_z_offset: u64,
        sequence_z_offset: u64,
        dictionary_offset: u64,
        triples_offset: u64,
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
            triples_offset,
        }
    }

    pub fn write_cache_from_hdt_file(hdt_path: &Path) -> Self {
        let mut reader = std::io::BufReader::new(std::fs::File::open(&hdt_path).expect("msg"));
        // Read control info (global header)
        ControlInfo::read(&mut reader).expect("msg");

        // Read header
        let _header = Header::read(&mut reader).expect("msg");

        // Track dictionary offset
        let dictionary_offset = reader.stream_position().expect("msg");

        // Read dictionary
        let unvalidated_dict = FourSectDict::read(&mut reader).expect("msg");
        let _dict = unvalidated_dict.validate().expect("msg");

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
        let c = Self::from_triples_bitmap(
            &triples_bitmap,
            bitmap_y_offset,
            bitmap_z_offset,
            sequence_z_offset,
            dictionary_offset,
            triples_offset,
        );
        let mut abs_path = std::fs::canonicalize(hdt_path).expect("msg");
        let _ = abs_path.pop();
        let index_file_name = format!("{}.{CACHE_EXT}", hdt_path.file_name().unwrap().to_str().unwrap());
        let index_file_path = abs_path.join(index_file_name);

        c.write_to_file(index_file_path).expect("msg");
        // // Generate cache from the already-loaded in-memory HDT
        // let cache = HybridCache::from_triples_bitmap(
        //     &hdt_in_memory.triples, sequence_z_offset, dictionary_offset, triples_offset,
        // );
        c
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

        // Write file offsets (bitmaps and sequences read from HDT on-demand)
        writer.write_all(&self.bitmap_y_offset.to_le_bytes())?;
        writer.write_all(&self.bitmap_z_offset.to_le_bytes())?;
        writer.write_all(&self.sequence_z_offset.to_le_bytes())?;
        writer.write_all(&self.dictionary_offset.to_le_bytes())?;
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
            return Err(format!("Unsupported cache version: expected {}, found {}", VERSION, version).into());
        }

        // Read order
        let mut order_byte = [0u8];
        reader.read_exact(&mut order_byte)?;
        let order = Order::try_from(order_byte[0] as u32).map_err(|e| format!("Invalid order: {:?}", e))?;

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
    fn test_cache_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        // Load TriplesBitmap
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let hdt = Hdt::read(BufReader::new(file))?;

        // Generate cache with example offsets
        let cache = HybridCache::from_triples_bitmap(
            &hdt.triples,
            1000,  // bitmap_y_offset
            2000,  // bitmap_z_offset
            12345, // sequence_z_offset
            10000, // dictionary_offset
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
        assert_eq!(cache.triples_offset, cache2.triples_offset);

        println!("✅ Cache roundtrip successful!");
        println!("   Cache file size: {} bytes", std::fs::metadata(cache_path)?.len());

        // Clean up
        std::fs::remove_file(cache_path)?;

        Ok(())
    }
}
