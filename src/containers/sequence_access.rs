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
    /// File path
    file_path: std::path::PathBuf,
    /// File offset where sequence data starts
    data_offset: u64,
    /// Number of entries
    entries: usize,
    /// Bits per entry
    bits_per_entry_val: usize,
    /// Cached file handle
    file: std::sync::Arc<std::sync::Mutex<std::io::BufReader<std::fs::File>>>,
}

impl FileBasedSequence {
    /// Create a file-based sequence
    pub fn new(
        file_path: std::path::PathBuf,
        data_offset: u64,
        entries: usize,
        bits_per_entry: usize,
    ) -> std::io::Result<Self> {
        let file = std::fs::File::open(&file_path)?;
        let reader = std::io::BufReader::new(file);

        Ok(Self {
            file_path,
            data_offset,
            entries,
            bits_per_entry_val: bits_per_entry,
            file: std::sync::Arc::new(std::sync::Mutex::new(reader)),
        })
    }

    /// Read a single value at index
    fn read_value(&self, index: usize) -> std::io::Result<usize> {
        use std::io::{Read, Seek, SeekFrom};

        if index >= self.entries {
            return Ok(0);
        }

        let mut reader = self.file.lock().unwrap();

        // Calculate bit position
        let bit_offset = index * self.bits_per_entry_val;
        let byte_offset = bit_offset / 8;
        let bit_in_byte = bit_offset % 8;

        // Seek to position
        reader.seek(SeekFrom::Start(self.data_offset + byte_offset as u64))?;

        // Read enough bytes
        let bytes_needed = ((self.bits_per_entry_val + bit_in_byte + 7) / 8).min(16);
        let mut buffer = vec![0u8; bytes_needed];
        reader.read_exact(&mut buffer)?;

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
