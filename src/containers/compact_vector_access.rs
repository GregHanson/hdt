//! Trait abstraction for compact vector access - allows both in-memory and file-based implementations

use std::fmt::Debug;
use sucds::int_vectors::Access;

/// Trait for accessing compact integer vectors
///
/// This abstraction allows compact vectors to be either:
/// - In-memory (sucds CompactVector)
/// - File-based (streaming from disk)
pub trait CompactVectorAccess: Debug + Send + Sync {
    /// Get the value at the given index
    fn get(&self, index: usize) -> usize;

    /// Number of entries in the vector
    fn len(&self) -> usize;

    /// Whether the vector is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Width (bits per element)
    fn width(&self) -> usize;

    /// Size in bytes (memory footprint)
    fn size_in_bytes(&self) -> usize;
}

/// In-memory compact vector implementation (wraps sucds CompactVector)
#[derive(Debug)]
pub struct InMemoryCompactVector {
    inner: sucds::int_vectors::CompactVector,
}

impl InMemoryCompactVector {
    /// Create a new InMemoryCompactVector from a CompactVector
    pub fn new(cv: sucds::int_vectors::CompactVector) -> Self {
        Self { inner: cv }
    }

    /// Get a reference to the inner CompactVector
    pub fn inner(&self) -> &sucds::int_vectors::CompactVector {
        &self.inner
    }

    /// Consume self and return the inner CompactVector
    pub fn into_inner(self) -> sucds::int_vectors::CompactVector {
        self.inner
    }
}

impl CompactVectorAccess for InMemoryCompactVector {
    fn get(&self, index: usize) -> usize {
        self.inner.access(index).unwrap_or(0)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn width(&self) -> usize {
        self.inner.width()
    }

    fn size_in_bytes(&self) -> usize {
        self.inner.len() * self.inner.width() / 8
    }
}

/// File-based compact vector implementation (streams from disk)
#[derive(Debug)]
pub struct FileBasedCompactVector {
    /// File path
    file_path: std::path::PathBuf,
    /// File offset where vector data starts
    data_offset: u64,
    /// Number of entries
    entries: usize,
    /// Bits per entry (width)
    width_val: usize,
    /// Cached file handle
    file: std::sync::Arc<std::sync::Mutex<std::io::BufReader<std::fs::File>>>,
}

impl FileBasedCompactVector {
    /// Create a file-based compact vector
    pub fn new(
        file_path: std::path::PathBuf,
        data_offset: u64,
        entries: usize,
        width: usize,
    ) -> std::io::Result<Self> {
        let file = std::fs::File::open(&file_path)?;
        let reader = std::io::BufReader::new(file);

        Ok(Self {
            file_path,
            data_offset,
            entries,
            width_val: width,
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
        let bit_offset = index * self.width_val;
        let byte_offset = bit_offset / 8;
        let bit_in_byte = bit_offset % 8;

        // Seek to position
        reader.seek(SeekFrom::Start(self.data_offset + byte_offset as u64))?;

        // Read enough bytes to extract the value
        let bytes_needed = ((self.width_val + bit_in_byte + 7) / 8).min(16);
        let mut buffer = vec![0u8; bytes_needed];
        reader.read_exact(&mut buffer)?;

        // Extract bits using the same logic as FileBasedSequence
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

        let result_shift = USIZE_BITS - self.width_val;
        let result = if bit_index + self.width_val <= USIZE_BITS {
            let block_shift = USIZE_BITS - bit_index - self.width_val;
            (data[block_index] << block_shift) >> result_shift
        } else {
            let block_shift = (USIZE_BITS << 1) - bit_index - self.width_val;
            let mut r = data[block_index] >> bit_index;
            if block_index + 1 < data.len() {
                r |= (data[block_index + 1] << block_shift) >> result_shift;
            }
            r
        };

        Ok(result)
    }
}

impl CompactVectorAccess for FileBasedCompactVector {
    fn get(&self, index: usize) -> usize {
        self.read_value(index).unwrap_or(0)
    }

    fn len(&self) -> usize {
        self.entries
    }

    fn width(&self) -> usize {
        self.width_val
    }

    fn size_in_bytes(&self) -> usize {
        // Only counting metadata, not the file
        std::mem::size_of::<Self>()
    }
}
