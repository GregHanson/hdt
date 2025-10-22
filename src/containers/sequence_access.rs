//! Trait abstraction for sequence access - allows both in-memory and file-based implementations

use std::fmt::Debug;

/// Trait for accessing integer sequences
///
/// This abstraction allows sequences to be either:
/// - In-memory (Sequence struct)
/// - File-based (streaming from disk)
pub trait SequenceAccess: Debug + Send + Sync {
    /// Get the value at the given index
    fn get(&self, index: usize) -> usize;

    /// Number of entries in the sequence
    fn len(&self) -> usize;

    /// Whether the sequence is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of bits per entry
    fn bits_per_entry(&self) -> usize;

    /// Size in bytes (memory footprint)
    fn size_in_bytes(&self) -> usize;
}

/// In-memory sequence implementation (existing Sequence)
#[derive(Debug)]
pub struct InMemorySequence {
    inner: crate::containers::Sequence,
}

impl InMemorySequence {
    pub fn new(sequence: crate::containers::Sequence) -> Self {
        Self { inner: sequence }
    }

    pub fn into_inner(self) -> crate::containers::Sequence {
        self.inner
    }

    pub fn inner(&self) -> &crate::containers::Sequence {
        &self.inner
    }
}

impl SequenceAccess for InMemorySequence {
    fn get(&self, index: usize) -> usize {
        self.inner.get(index)
    }

    fn len(&self) -> usize {
        self.inner.entries
    }

    fn bits_per_entry(&self) -> usize {
        self.inner.bits_per_entry
    }

    fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }
}

/// File-based sequence implementation (streams from disk)
#[derive(Debug)]
pub struct FileBasedSequence {
    /// File offset where sequence data starts
    data_offset: u64,
    /// Number of entries
    entries: usize,
    /// Bits per entry
    bits_per_entry_val: usize,
    /// Cached file handle with position tracking
    file: std::sync::Arc<std::sync::Mutex<PositionedReader>>,
}

/// Wrapper around BufReader that tracks the current position
#[derive(Debug)]
struct PositionedReader {
    reader: std::io::BufReader<std::fs::File>,
    /// Current position in the file (accounting for buffering)
    position: u64,
}

impl PositionedReader {
    fn new(reader: std::io::BufReader<std::fs::File>) -> Self {
        Self { reader, position: 0 }
    }

    /// Seek to an absolute position, using relative seeking when possible
    fn seek_to(&mut self, target: u64) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom};

        if self.position == target {
            // Already at the right position
            return Ok(());
        }

        // Calculate relative offset
        let offset = target as i64 - self.position as i64;

        // Use relative seek for efficiency
        self.reader.seek(SeekFrom::Current(offset))?;
        self.position = target;
        Ok(())
    }

    /// Read exact number of bytes and update position
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        use std::io::Read;
        self.reader.read_exact(buf)?;
        self.position += buf.len() as u64;
        Ok(())
    }
}

impl FileBasedSequence {
    /// Create a file-based sequence
    ///
    /// # Arguments
    /// * `file_path` - Path to the HDT file
    /// * `sequence_offset` - File offset to the START of the sequence section (including metadata)
    /// * `expected_entries` - Expected number of entries (for validation)
    /// * `expected_bits_per_entry` - Expected bits per entry (for validation)
    ///
    /// The function will read and validate the metadata, then calculate the actual data offset.
    pub fn new(
        file_path: std::path::PathBuf, sequence_offset: u64, expected_entries: usize,
        expected_bits_per_entry: usize,
    ) -> std::io::Result<Self> {
        use crate::containers::vbyte::read_vbyte;
        use std::io::{Read, Seek, SeekFrom};

        let file = std::fs::File::open(&file_path)?;
        let mut reader = std::io::BufReader::new(file);

        // Seek to the start of the sequence section
        reader.seek(SeekFrom::Start(sequence_offset))?;

        // Track how many bytes we read for metadata
        let mut metadata_size = 0u64;

        // Read and validate type (1 byte)
        let mut type_buf = [0u8];
        reader.read_exact(&mut type_buf)?;
        metadata_size += 1;
        if type_buf[0] != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported sequence type: {}, expected 1 (Log64)", type_buf[0]),
            ));
        }

        // Read bits_per_entry (1 byte)
        let mut bits_buf = [0u8];
        reader.read_exact(&mut bits_buf)?;
        metadata_size += 1;
        let bits_per_entry = bits_buf[0] as usize;

        // Validate bits_per_entry matches expected
        if bits_per_entry != expected_bits_per_entry {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Bits per entry mismatch: found {} in file, expected {}",
                    bits_per_entry, expected_bits_per_entry
                ),
            ));
        }

        // Read entries (variable-length vbyte)
        let (entries, vbyte_bytes) = read_vbyte(&mut reader)?;
        metadata_size += vbyte_bytes.len() as u64;

        // Validate entries matches expected
        if entries != expected_entries {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Entry count mismatch: found {} in file, expected {}", entries, expected_entries),
            ));
        }

        // Skip CRC8 checksum (1 byte) - we don't validate it here for performance
        let mut crc_buf = [0u8];
        reader.read_exact(&mut crc_buf)?;
        metadata_size += 1;

        // Calculate the actual data offset (sequence_offset + metadata_size)
        let data_offset = sequence_offset + metadata_size;

        // Re-open file for the cached reader
        let file = std::fs::File::open(&file_path)?;
        let reader = std::io::BufReader::new(file);
        let positioned_reader = PositionedReader::new(reader);

        Ok(Self {
            data_offset,
            entries,
            bits_per_entry_val: bits_per_entry,
            file: std::sync::Arc::new(std::sync::Mutex::new(positioned_reader)),
        })
    }

    /// Read a single value at index
    fn read_value(&self, index: usize) -> std::io::Result<usize> {
        if index >= self.entries {
            return Ok(0);
        }

        let mut positioned_reader = self.file.lock().unwrap();

        // Calculate bit position
        let bit_offset = index * self.bits_per_entry_val;
        let byte_offset = bit_offset / 8;
        let bit_in_byte = bit_offset % 8;

        // Seek to position using optimized relative seeking
        let target_position = self.data_offset + byte_offset as u64;
        positioned_reader.seek_to(target_position)?;

        // Read enough bytes
        let bytes_needed = ((self.bits_per_entry_val + bit_in_byte + 7) / 8).min(16);
        let mut buffer = vec![0u8; bytes_needed];
        positioned_reader.read_exact(&mut buffer)?;

        // Extract bits
        let mut data = Vec::new();
        for chunk in buffer.chunks(std::mem::size_of::<usize>().min(buffer.len())) {
            let mut val = 0usize;
            for (i, &byte) in chunk.iter().enumerate() {
                val |= (byte as usize) << (i * 8);
            }
            data.push(val);
        }

        const USIZE_BITS: usize = usize::BITS as usize;
        let scaled_index_in_buffer = bit_in_byte;
        let block_index = scaled_index_in_buffer / USIZE_BITS;
        let bit_index = scaled_index_in_buffer % USIZE_BITS;

        let result_shift = USIZE_BITS - self.bits_per_entry_val;
        let result = if bit_index + self.bits_per_entry_val <= USIZE_BITS {
            let block_shift = USIZE_BITS - bit_index - self.bits_per_entry_val;
            (data[block_index] << block_shift) >> result_shift
        } else {
            let block_shift = (USIZE_BITS << 1) - bit_index - self.bits_per_entry_val;
            let mut r = data[block_index] >> bit_index;
            if block_index + 1 < data.len() {
                r |= (data[block_index + 1] << block_shift) >> result_shift;
            }
            r
        };

        Ok(result)
    }
}

impl SequenceAccess for FileBasedSequence {
    fn get(&self, index: usize) -> usize {
        self.read_value(index).unwrap_or(0)
    }

    fn len(&self) -> usize {
        self.entries
    }

    fn bits_per_entry(&self) -> usize {
        self.bits_per_entry_val
    }

    fn size_in_bytes(&self) -> usize {
        // Only counting metadata, not the file
        std::mem::size_of::<Self>()
    }
}
