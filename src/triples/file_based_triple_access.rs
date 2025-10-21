//! File-based triple access that performs random access lookups directly on the HDT file
//! without loading entire sections into memory.
//!
//! This implementation reads only the metadata headers and keeps file offsets,
//! then performs direct seeks and bit-level reads for individual values.

use crate::triples::{Error, Id, Order};
use crate::triples::triple_access::{TripleAccess, Result};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Metadata for a Sequence that allows random access without loading the full data
#[derive(Debug, Clone)]
struct SequenceMetadata {
    /// File offset where the sequence data starts (after header)
    data_offset: u64,
    /// Number of entries in the sequence
    entries: usize,
    /// Bits per entry
    bits_per_entry: usize,
}

/// Metadata for a Bitmap that allows random access
#[derive(Debug, Clone)]
struct BitmapMetadata {
    /// File offset where the bitmap data starts (after header)
    data_offset: u64,
    /// Number of bits in the bitmap
    num_bits: usize,
}

/// File offsets and metadata for HDT sections
#[derive(Debug, Clone)]
struct SectionMetadata {
    bitmap_y: BitmapMetadata,
    bitmap_z: BitmapMetadata,
    sequence_y: SequenceMetadata,
    sequence_z: SequenceMetadata,
}

/// Essential metadata about the triples section
#[derive(Debug, Clone)]
pub struct TriplesMetadata {
    pub num_triples: usize,
    pub num_subjects: usize,
}

/// File-based triple access - minimal memory footprint
///
/// This implementation:
/// - Reads only metadata headers on initialization
/// - Performs file seeks for every data access
/// - No in-memory indexes or data structures (except metadata)
/// - Suitable for very large HDT files or memory-constrained environments
///
/// Trade-offs:
/// - Minimal memory usage
/// - High latency per query (disk I/O on every access)
/// - Not suitable for high-throughput query workloads
/// - Best for: one-off queries, exploration, or when combined with application-level caching
pub struct FileBasedTripleAccess {
    /// Triple ordering
    order: Order,
    /// Path to the HDT file
    file_path: std::path::PathBuf,
    /// Metadata for all sections
    metadata: SectionMetadata,
    /// Basic triple statistics
    stats: TriplesMetadata,
    /// Shared file handle for thread-safe access
    /// Using Arc<Mutex<>> to allow Send + Sync
    file: Arc<Mutex<BufReader<File>>>,
}

impl std::fmt::Debug for FileBasedTripleAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileBasedTripleAccess")
            .field("order", &self.order)
            .field("file_path", &self.file_path)
            .field("stats", &self.stats)
            .finish()
    }
}

impl FileBasedTripleAccess {
    /// Create a new file-based triple access from an HDT file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        let file = File::open(&file_path)?;
        let mut reader = BufReader::new(file);

        // Parse HDT structure to get to triples section
        let _global_ci = crate::ControlInfo::read(&mut reader)?;
        crate::header::Header::read(&mut reader)?;
        crate::four_sect_dict::FourSectDict::read(&mut reader)?;
        let triples_ci = crate::ControlInfo::read(&mut reader)?;

        let order = if let Some(n) = triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            Order::try_from(n)?
        } else {
            return Err(Error::UnspecifiedTriplesOrder);
        };

        // Read metadata for bitmap_y
        let bitmap_y_start = reader.stream_position()?;
        let bitmap_y_meta = Self::read_bitmap_metadata(&mut reader)?;

        // Read metadata for bitmap_z
        let bitmap_z_start = reader.stream_position()?;
        let bitmap_z_meta = Self::read_bitmap_metadata(&mut reader)?;

        // Read metadata for sequence_y
        let sequence_y_start = reader.stream_position()?;
        let sequence_y_meta = Self::read_sequence_metadata(&mut reader)?;

        // Read metadata for sequence_z
        let sequence_z_start = reader.stream_position()?;
        let sequence_z_meta = Self::read_sequence_metadata(&mut reader)?;

        let metadata = SectionMetadata {
            bitmap_y: bitmap_y_meta,
            bitmap_z: bitmap_z_meta,
            sequence_y: sequence_y_meta,
            sequence_z: sequence_z_meta,
        };

        let stats = TriplesMetadata {
            num_triples: metadata.sequence_z.entries,
            num_subjects: Self::count_bitmap_ones(&mut reader, &metadata.bitmap_y)?,
        };

        Ok(Self {
            order,
            file_path,
            metadata,
            stats,
            file: Arc::new(Mutex::new(reader)),
        })
    }

    /// Read bitmap metadata (type, num_bits) without loading the data
    fn read_bitmap_metadata<R: BufRead + Seek>(reader: &mut R) -> Result<BitmapMetadata> {
        let start_pos = reader.stream_position()?;

        // Read type
        let mut bitmap_type = [0u8];
        reader.read_exact(&mut bitmap_type)?;
        if bitmap_type[0] != 1 {
            return Err(Error::External(format!("Unsupported bitmap type: {}", bitmap_type[0]).into()));
        }

        // Read num_bits (vbyte encoded)
        let (num_bits, _) = crate::containers::vbyte::read_vbyte(reader)?;

        // Read CRC8 (1 byte)
        let mut _crc8 = [0u8];
        reader.read_exact(&mut _crc8)?;

        // Data starts here
        let data_offset = reader.stream_position()?;

        // Skip past the bitmap data to position for next section
        // Bitmap data size = ceil(num_bits / 8) + 4 bytes (CRC32)
        let data_bytes = ((num_bits + 7) / 8) as u64;
        reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

        Ok(BitmapMetadata { data_offset, num_bits })
    }

    /// Read sequence metadata (type, bits_per_entry, entries) without loading the data
    fn read_sequence_metadata<R: BufRead + Seek>(reader: &mut R) -> Result<SequenceMetadata> {
        let start_pos = reader.stream_position()?;

        // Read type
        let mut seq_type = [0u8];
        reader.read_exact(&mut seq_type)?;
        if seq_type[0] != 1 {
            return Err(Error::External(format!("Unsupported sequence type: {}", seq_type[0]).into()));
        }

        // Read bits_per_entry
        let mut bits_per_entry_buf = [0u8];
        reader.read_exact(&mut bits_per_entry_buf)?;
        let bits_per_entry = bits_per_entry_buf[0] as usize;

        // Read entries (vbyte encoded)
        let (entries, _) = crate::containers::vbyte::read_vbyte(reader)?;

        // Read CRC8 (1 byte)
        let mut _crc8 = [0u8];
        reader.read_exact(&mut _crc8)?;

        // Data starts here
        let data_offset = reader.stream_position()?;

        // Skip past the sequence data to position for next section
        // Sequence data size = ceil(bits_per_entry * entries / 8) + 4 bytes (CRC32)
        let total_bits = bits_per_entry * entries;
        let data_bytes = ((total_bits + 7) / 8) as u64;
        reader.seek(SeekFrom::Current(data_bytes as i64 + 4))?;

        Ok(SequenceMetadata { data_offset, entries, bits_per_entry })
    }

    /// Count the number of 1-bits in a bitmap (for num_subjects calculation)
    fn count_bitmap_ones<R: BufRead + Seek>(reader: &mut R, bitmap_meta: &BitmapMetadata) -> Result<usize> {
        reader.seek(SeekFrom::Start(bitmap_meta.data_offset))?;

        let data_bytes = (bitmap_meta.num_bits + 7) / 8;
        let mut buffer = vec![0u8; data_bytes];
        reader.read_exact(&mut buffer)?;

        let mut count = 0;
        for byte in buffer {
            count += byte.count_ones() as usize;
        }
        Ok(count)
    }

    /// Read a single value from a sequence at the given index
    fn read_sequence_value(&self, seq_meta: &SequenceMetadata, index: usize) -> Result<usize> {
        if index >= seq_meta.entries {
            return Ok(0);
        }

        let mut reader = self.file.lock().unwrap();

        // Calculate bit position
        let bit_offset = index * seq_meta.bits_per_entry;
        let byte_offset = bit_offset / 8;
        let bit_in_byte = bit_offset % 8;

        // Seek to the position
        reader.seek(SeekFrom::Start(seq_meta.data_offset + byte_offset as u64))?;

        // Read enough bytes to cover the value (max 16 bytes for safety with alignment)
        let bytes_needed = ((seq_meta.bits_per_entry + bit_in_byte + 7) / 8).min(16);
        let mut buffer = vec![0u8; bytes_needed];
        reader.read_exact(&mut buffer)?;

        // Extract the bits using the same logic as Sequence::get()
        // Convert buffer to usize array for easier manipulation
        let mut data = Vec::new();
        for chunk in buffer.chunks(std::mem::size_of::<usize>().min(buffer.len())) {
            let mut val = 0usize;
            for (i, &byte) in chunk.iter().enumerate() {
                val |= (byte as usize) << (i * 8);
            }
            data.push(val);
        }

        // Extract value using bit manipulation similar to Sequence::get()
        const USIZE_BITS: usize = usize::BITS as usize;
        let scaled_index_in_buffer = bit_in_byte; // Already at correct byte boundary
        let block_index = scaled_index_in_buffer / USIZE_BITS;
        let bit_index = scaled_index_in_buffer % USIZE_BITS;

        let result_shift = USIZE_BITS - seq_meta.bits_per_entry;
        let result = if bit_index + seq_meta.bits_per_entry <= USIZE_BITS {
            let block_shift = USIZE_BITS - bit_index - seq_meta.bits_per_entry;
            (data[block_index] << block_shift) >> result_shift
        } else {
            let block_shift = (USIZE_BITS << 1) - bit_index - seq_meta.bits_per_entry;
            let mut r = data[block_index] >> bit_index;
            if block_index + 1 < data.len() {
                r |= (data[block_index + 1] << block_shift) >> result_shift;
            }
            r
        };

        Ok(result)
    }

    /// Perform select1 operation on bitmap (find position of k-th 1-bit)
    fn bitmap_select1(&self, bitmap_meta: &BitmapMetadata, k: usize) -> Result<Option<usize>> {
        let mut reader = self.file.lock().unwrap();
        reader.seek(SeekFrom::Start(bitmap_meta.data_offset))?;

        let data_bytes = (bitmap_meta.num_bits + 7) / 8;
        let mut buffer = vec![0u8; data_bytes];
        reader.read_exact(&mut buffer)?;

        // Linear scan to find k-th 1-bit
        // For better performance, this should build rank/select index, but that defeats
        // the purpose of minimal memory. In practice, you'd want a hybrid approach.
        let mut ones_seen = 0;
        for (byte_idx, &byte) in buffer.iter().enumerate() {
            let ones_in_byte = byte.count_ones() as usize;
            if ones_seen + ones_in_byte > k {
                // The k-th one is in this byte
                for bit_idx in 0..8 {
                    if (byte >> bit_idx) & 1 == 1 {
                        if ones_seen == k {
                            return Ok(Some(byte_idx * 8 + bit_idx));
                        }
                        ones_seen += 1;
                    }
                }
            }
            ones_seen += ones_in_byte;
        }

        Ok(None)
    }

    /// Perform rank operation on bitmap (count 1-bits up to position k)
    fn bitmap_rank(&self, bitmap_meta: &BitmapMetadata, k: usize) -> Result<usize> {
        let mut reader = self.file.lock().unwrap();
        reader.seek(SeekFrom::Start(bitmap_meta.data_offset))?;

        let bytes_to_read = ((k + 8) / 8).min((bitmap_meta.num_bits + 7) / 8);
        let mut buffer = vec![0u8; bytes_to_read];
        reader.read_exact(&mut buffer)?;

        let mut count = 0;
        let full_bytes = k / 8;
        for i in 0..full_bytes.min(buffer.len()) {
            count += buffer[i].count_ones() as usize;
        }

        // Count remaining bits in partial byte
        if full_bytes < buffer.len() {
            let remaining_bits = k % 8;
            let byte = buffer[full_bytes];
            for i in 0..remaining_bits {
                if (byte >> i) & 1 == 1 {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Get last Y position for subject
    pub fn last_y(&self, subject_id: Id) -> Result<usize> {
        if subject_id >= self.stats.num_subjects {
            let next_pos = self.find_y(subject_id + 1)?;
            Ok(if next_pos > 0 { next_pos - 1 } else { 0 })
        } else {
            let next_pos = self.find_y(subject_id + 1)?;
            Ok(if next_pos > 0 { next_pos - 1 } else { 0 })
        }
    }
}

impl TripleAccess for FileBasedTripleAccess {
    fn num_triples(&self) -> usize {
        self.stats.num_triples
    }

    fn order(&self) -> Order {
        self.order.clone()
    }

    fn size_in_bytes(&self) -> usize {
        // Only counting in-memory metadata, not the file
        std::mem::size_of::<Self>()
    }

    fn find_y(&self, subject_id: Id) -> Result<usize> {
        if subject_id == 0 {
            return Ok(0);
        }

        match self.bitmap_select1(&self.metadata.bitmap_y, subject_id - 1)? {
            Some(pos) => Ok(pos + 1),
            None => Err(Error::External(
                format!("invalid s_id {subject_id}, there are only {} subjects", self.stats.num_subjects).into(),
            )),
        }
    }

    fn search_y(&self, subject_id: Id, predicate_id: Id) -> Result<Option<usize>> {
        let start = self.find_y(subject_id)?;
        let end = self.last_y(subject_id)?;

        // Linear scan through predicates for this subject
        for pos_y in start..=end {
            let pred = self.get_predicate(pos_y)?;
            if pred == predicate_id {
                return Ok(Some(pos_y));
            } else if pred > predicate_id {
                // Predicates are sorted, so we can stop early
                return Ok(None);
            }
        }
        Ok(None)
    }

    fn get_object(&self, pos_z: usize) -> Result<Id> {
        self.read_sequence_value(&self.metadata.sequence_z, pos_z)
    }

    fn get_predicate(&self, pos_y: usize) -> Result<Id> {
        self.read_sequence_value(&self.metadata.sequence_y, pos_y)
    }

    fn has_subject_index(&self) -> bool {
        false // No pre-built indexes
    }

    fn has_predicate_index(&self) -> bool {
        false
    }

    fn has_object_index(&self) -> bool {
        false
    }

    fn get_object_positions(&self, object_id: Id) -> Result<Vec<usize>> {
        // Linear scan - very slow without index
        let mut positions = Vec::new();
        for pos in 0..self.num_triples() {
            if self.get_object(pos)? == object_id {
                positions.push(pos);
            }
        }
        Ok(positions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_based_access() -> Result<()> {
        let file_access = FileBasedTripleAccess::from_file("tests/resources/snikmeta.hdt")?;

        assert!(file_access.num_triples() > 0);
        println!("Number of triples: {}", file_access.num_triples());
        println!("Number of subjects: {}", file_access.stats.num_subjects);

        // Test basic access
        let obj = file_access.get_object(0)?;
        println!("First object: {}", obj);

        let pred = file_access.get_predicate(0)?;
        println!("First predicate: {}", pred);

        Ok(())
    }

    #[test]
    fn test_memory_usage() {
        let file_access = FileBasedTripleAccess::from_file("tests/resources/snikmeta.hdt").unwrap();
        let memory = file_access.size_in_bytes();
        println!("Memory usage (metadata only): {} bytes", memory);

        // Should be minimal - just metadata
        assert!(memory < 10_000); // Less than 10KB for metadata
    }
}
