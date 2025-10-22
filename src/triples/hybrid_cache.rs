//! Cache file format for HybridTripleAccess
//!
//! This module provides functionality to serialize/deserialize the in-memory
//! structures used by HybridTripleAccess, allowing them to be prebuilt from
//! TriplesBitmap and reused.
//!
//! Cache file format (.hdt.cache):
//! ```text
//! [Magic: "HDTCACHE"]               (8 bytes)
//! [Version: u32]                    (4 bytes)
//! [Order: u8]                       (1 byte)
//! [Bitmap Y]                        (variable - sucds serialized)
//! [Adjlist Z Bitmap]                (variable - sucds serialized)
//! [Op Index Bitmap]                 (variable - sucds serialized)
//! [Op Index Sequence]               (variable - sucds serialized CompactVector)
//! [Wavelet Y]                       (variable - sucds serialized)
//! [Sequence Z Offset: u64]          (8 bytes - offset in HDT file where sequence_z begins)
//! [Sequence Z Entries: usize]       (8 bytes - number of entries in sequence_z)
//! [Sequence Z Bits Per Entry: usize](8 bytes - bits per entry in sequence_z)
//! [Dictionary Offset: u64]          (8 bytes - offset in HDT file where Dictionary section begins)
//! [Triples Offset: u64]             (8 bytes - offset in HDT file where Triples section begins)
//! [CRC32]                           (4 bytes)
//! ```

use crate::containers::Bitmap;
use crate::containers::SequenceAccess;
use crate::triples::{Order, TriplesBitmap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use sucds::Serializable;
use sucds::bit_vectors::Rank9Sel;
use sucds::char_sequences::WaveletMatrix;
use sucds::int_vectors::CompactVector;

const MAGIC: &[u8; 8] = b"HDTCACHE";
const VERSION: u32 = 1;

/// Cached structures for HybridTripleAccess
#[derive(Debug)]
pub struct HybridCache {
    pub order: Order,
    pub bitmap_y: Bitmap,
    pub adjlist_z_bitmap: Bitmap,
    pub op_index_bitmap: Bitmap,
    pub op_index_sequence: CompactVector,
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
    pub sequence_z_offset: u64,
    pub sequence_z_entries: usize,
    pub sequence_z_bits_per_entry: usize,
    pub dictionary_offset: u64,
    pub triples_offset: u64,
}

impl HybridCache {
    /// Generate cache from TriplesBitmap
    pub fn from_triples_bitmap(
        triples: &TriplesBitmap,
        sequence_z_offset: u64,
        dictionary_offset: u64,
        triples_offset: u64,
    ) -> Self {
        Self {
            order: triples.order.clone(),
            bitmap_y: triples.bitmap_y.clone(),
            adjlist_z_bitmap: triples.adjlist_z.bitmap.clone(),
            op_index_bitmap: triples.op_index.bitmap.clone(),
            op_index_sequence: triples.op_index.sequence.clone(),
            wavelet_y: triples.wavelet_y.clone(),
            sequence_z_offset,
            sequence_z_entries: triples.adjlist_z.sequence.len(),
            sequence_z_bits_per_entry: triples.adjlist_z.sequence.bits_per_entry(),
            dictionary_offset,
            triples_offset,
        }
    }

    /// Write cache to file
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write magic
        writer.write_all(MAGIC)?;

        // Write version
        writer.write_all(&VERSION.to_le_bytes())?;

        // Write order
        writer.write_all(&[self.order.clone() as u8])?;

        // Write bitmap_y
        self.bitmap_y.dict.serialize_into(&mut writer)?;

        // Write adjlist_z.bitmap
        self.adjlist_z_bitmap.dict.serialize_into(&mut writer)?;

        // Write op_index.bitmap
        self.op_index_bitmap.dict.serialize_into(&mut writer)?;

        // Write op_index.sequence
        self.op_index_sequence.serialize_into(&mut writer)?;

        // Write wavelet_y
        self.wavelet_y.serialize_into(&mut writer)?;

        // Write sequence_z metadata
        writer.write_all(&self.sequence_z_offset.to_le_bytes())?;
        writer.write_all(&self.sequence_z_entries.to_le_bytes())?;
        writer.write_all(&self.sequence_z_bits_per_entry.to_le_bytes())?;

        // Write other file offsets
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
            return Err(format!("Unsupported cache version: {}", version).into());
        }

        // Read order
        let mut order_byte = [0u8];
        reader.read_exact(&mut order_byte)?;
        let order = Order::try_from(order_byte[0] as u32).map_err(|e| format!("Invalid order: {:?}", e))?;

        // Read bitmap_y
        let bitmap_y_dict = Rank9Sel::deserialize_from(&mut reader)?;
        let bitmap_y = Bitmap { dict: bitmap_y_dict };

        // Read adjlist_z.bitmap
        let adjlist_z_bitmap_dict = Rank9Sel::deserialize_from(&mut reader)?;
        let adjlist_z_bitmap = Bitmap { dict: adjlist_z_bitmap_dict };

        // Read op_index.bitmap
        let op_index_bitmap_dict = Rank9Sel::deserialize_from(&mut reader)?;
        let op_index_bitmap = Bitmap { dict: op_index_bitmap_dict };

        // Read op_index.sequence
        let op_index_sequence = CompactVector::deserialize_from(&mut reader)?;

        // Read wavelet_y
        let wavelet_y = WaveletMatrix::deserialize_from(&mut reader)?;

        // Read sequence_z metadata
        let mut sequence_z_offset_bytes = [0u8; 8];
        reader.read_exact(&mut sequence_z_offset_bytes)?;
        let sequence_z_offset = u64::from_le_bytes(sequence_z_offset_bytes);

        let mut sequence_z_entries_bytes = [0u8; 8];
        reader.read_exact(&mut sequence_z_entries_bytes)?;
        let sequence_z_entries = usize::from_le_bytes(sequence_z_entries_bytes);

        let mut sequence_z_bits_per_entry_bytes = [0u8; 8];
        reader.read_exact(&mut sequence_z_bits_per_entry_bytes)?;
        let sequence_z_bits_per_entry = usize::from_le_bytes(sequence_z_bits_per_entry_bytes);

        // Read other file offsets
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
            bitmap_y,
            adjlist_z_bitmap,
            op_index_bitmap,
            op_index_sequence,
            wavelet_y,
            sequence_z_offset,
            sequence_z_entries,
            sequence_z_bits_per_entry,
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
            12345,  // sequence_z_offset
            10000,  // dictionary_offset
            20000,  // triples_offset
        );

        // Write to file
        let cache_path = "/tmp/test.hdt.cache";
        cache.write_to_file(cache_path)?;

        // Read back
        let cache2 = HybridCache::read_from_file(cache_path)?;

        // Verify
        assert_eq!(cache.order as u8, cache2.order as u8);
        assert_eq!(cache.bitmap_y.len(), cache2.bitmap_y.len());
        assert_eq!(cache.adjlist_z_bitmap.len(), cache2.adjlist_z_bitmap.len());
        assert_eq!(cache.op_index_bitmap.len(), cache2.op_index_bitmap.len());
        assert_eq!(cache.op_index_sequence.len(), cache2.op_index_sequence.len());
        assert_eq!(cache.wavelet_y.len(), cache2.wavelet_y.len());
        assert_eq!(cache.sequence_z_offset, cache2.sequence_z_offset);
        assert_eq!(cache.sequence_z_entries, cache2.sequence_z_entries);
        assert_eq!(cache.sequence_z_bits_per_entry, cache2.sequence_z_bits_per_entry);
        assert_eq!(cache.dictionary_offset, cache2.dictionary_offset);
        assert_eq!(cache.triples_offset, cache2.triples_offset);

        println!("✅ Cache roundtrip successful!");
        println!("   Cache file size: {} bytes", std::fs::metadata(cache_path)?.len());

        // Clean up
        std::fs::remove_file(cache_path)?;

        Ok(())
    }
}
