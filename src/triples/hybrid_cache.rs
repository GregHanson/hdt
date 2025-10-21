//! Cache file format for HybridTripleAccess
//!
//! This module provides functionality to serialize/deserialize the in-memory
//! structures used by HybridTripleAccess, allowing them to be prebuilt from
//! TriplesBitmap and reused.
//!
//! Cache file format (.hdt.cache):
//! ```text
//! [Magic: "HDTCACHE"]  (8 bytes)
//! [Version: u32]        (4 bytes)
//! [Order: u8]           (1 byte)
//! [Bitmap Y]            (variable - sucds serialized)
//! [Bitmap Z]            (variable - sucds serialized)
//! [Wavelet Y]           (variable - sucds serialized)
//! [Op Index Bitmap]     (variable - sucds serialized)
//! [Adjlist Z Metadata]  (24 bytes: offset u64, entries usize, bits usize)
//! [CRC32]               (4 bytes)
//! ```

use crate::triples::{Error, Order, TriplesBitmap};
use crate::containers::Bitmap;
use std::io::{Read, Write, BufReader, BufWriter};
use std::fs::File;
use std::path::Path;
use sucds::Serializable;
use sucds::bit_vectors::Rank9Sel;
use sucds::char_sequences::WaveletMatrix;

const MAGIC: &[u8; 8] = b"HDTCACHE";
const VERSION: u32 = 1;

/// Metadata about the cache file
#[derive(Debug, Clone)]
pub struct CacheMetadata {
    pub order: Order,
    pub adjlist_z_offset: u64,
    pub adjlist_z_entries: usize,
    pub adjlist_z_bits_per_entry: usize,
}

/// Cached structures for HybridTripleAccess
#[derive(Debug)]
pub struct HybridCache {
    pub bitmap_y: Bitmap,
    pub bitmap_z: Bitmap,
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
    pub op_index_bitmap: Bitmap,
    pub metadata: CacheMetadata,
}

impl HybridCache {
    /// Generate cache from TriplesBitmap
    pub fn from_triples_bitmap(triples: &TriplesBitmap, adjlist_z_offset: u64, adjlist_z_entries: usize, adjlist_z_bits_per_entry: usize) -> Self {
        Self {
            bitmap_y: triples.bitmap_y.clone(),
            bitmap_z: triples.adjlist_z.bitmap.clone(),
            wavelet_y: triples.wavelet_y.clone(),
            op_index_bitmap: triples.op_index.bitmap.clone(),
            metadata: CacheMetadata {
                order: triples.order.clone(),
                adjlist_z_offset,
                adjlist_z_entries,
                adjlist_z_bits_per_entry,
            },
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
        writer.write_all(&[self.metadata.order.clone() as u8])?;

        // Write bitmap_y
        self.bitmap_y.dict.serialize_into(&mut writer)?;

        // Write bitmap_z
        self.bitmap_z.dict.serialize_into(&mut writer)?;

        // Write wavelet_y
        self.wavelet_y.serialize_into(&mut writer)?;

        // Write op_index.bitmap
        self.op_index_bitmap.dict.serialize_into(&mut writer)?;

        // Write adjlist_z metadata
        writer.write_all(&self.metadata.adjlist_z_offset.to_le_bytes())?;
        writer.write_all(&self.metadata.adjlist_z_entries.to_le_bytes())?;
        writer.write_all(&self.metadata.adjlist_z_bits_per_entry.to_le_bytes())?;

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
        let order = Order::try_from(order_byte[0] as u32)
            .map_err(|e| format!("Invalid order: {:?}", e))?;

        // Read bitmap_y
        let bitmap_y_dict = Rank9Sel::deserialize_from(&mut reader)?;
        let bitmap_y = Bitmap { dict: bitmap_y_dict };

        // Read bitmap_z
        let bitmap_z_dict = Rank9Sel::deserialize_from(&mut reader)?;
        let bitmap_z = Bitmap { dict: bitmap_z_dict };

        // Read wavelet_y
        let wavelet_y = WaveletMatrix::deserialize_from(&mut reader)?;

        // Read op_index.bitmap
        let op_index_bitmap_dict = Rank9Sel::deserialize_from(&mut reader)?;
        let op_index_bitmap = Bitmap { dict: op_index_bitmap_dict };

        // Read adjlist_z metadata
        let mut offset_bytes = [0u8; 8];
        reader.read_exact(&mut offset_bytes)?;
        let adjlist_z_offset = u64::from_le_bytes(offset_bytes);

        let mut entries_bytes = [0u8; 8];
        reader.read_exact(&mut entries_bytes)?;
        let adjlist_z_entries = usize::from_le_bytes(entries_bytes);

        let mut bits_bytes = [0u8; 8];
        reader.read_exact(&mut bits_bytes)?;
        let adjlist_z_bits_per_entry = usize::from_le_bytes(bits_bytes);

        // Read CRC32 (skip validation for now)
        let mut _crc_bytes = [0u8; 4];
        reader.read_exact(&mut _crc_bytes)?;

        Ok(Self {
            bitmap_y,
            bitmap_z,
            wavelet_y,
            op_index_bitmap,
            metadata: CacheMetadata {
                order,
                adjlist_z_offset,
                adjlist_z_entries,
                adjlist_z_bits_per_entry,
            },
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

        // Generate cache
        let cache = HybridCache::from_triples_bitmap(&hdt.triples, 12345, 328, 8);

        // Write to file
        let cache_path = "/tmp/test.hdt.cache";
        cache.write_to_file(cache_path)?;

        // Read back
        let cache2 = HybridCache::read_from_file(cache_path)?;

        // Verify
        assert_eq!(cache.metadata.order as u8, cache2.metadata.order as u8);
        assert_eq!(cache.bitmap_y.len(), cache2.bitmap_y.len());
        assert_eq!(cache.bitmap_z.len(), cache2.bitmap_z.len());
        assert_eq!(cache.wavelet_y.len(), cache2.wavelet_y.len());

        println!("✅ Cache roundtrip successful!");
        println!("   Cache file size: {} bytes", std::fs::metadata(cache_path)?.len());

        // Clean up
        std::fs::remove_file(cache_path)?;

        Ok(())
    }
}
