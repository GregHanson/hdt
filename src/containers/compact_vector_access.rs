//! Access trait and implementations for CompactVector-like structures.
//!
//! This module provides trait-based abstraction over different storage strategies
//! for compact integer vectors (bit-packed arrays), similar to BitmapAccess but for
//! sucds::CompactVector structures.

use bytesize::ByteSize;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use sucds::int_vectors::{Access, CompactVector};
use sucds::Serializable;

/// Trait for accessing compact vector data with different storage strategies.
///
/// Implementations:
/// - `InMemoryCompactVector`: Fully loaded into memory (fast, high memory)
/// - `FileBasedCompactVector`: Read from file on each access (slow, minimal memory)
pub trait CompactVectorAccess: fmt::Debug + Send + Sync {
    /// Access the element at the given position.
    /// Returns None if the position is out of bounds.
    fn access(&self, pos: usize) -> Option<usize>;

    /// Returns the number of elements in the vector.
    fn len(&self) -> usize;

    /// Returns true if the vector is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of bits used for each element.
    fn width(&self) -> usize;

    /// Returns the size in bytes occupied by this structure.
    fn size_in_bytes(&self) -> usize;
}

/// In-memory compact vector implementation.
///
/// Wraps a sucds::CompactVector and keeps all data in memory.
/// Fast access but high memory usage.
#[derive(Clone)]
pub struct InMemoryCompactVector {
    inner: CompactVector,
}

impl InMemoryCompactVector {
    /// Create a new in-memory compact vector from a sucds::CompactVector.
    pub fn new(cv: CompactVector) -> Self {
        Self { inner: cv }
    }

    /// Get a reference to the inner CompactVector.
    pub fn inner(&self) -> &CompactVector {
        &self.inner
    }
}

impl fmt::Debug for InMemoryCompactVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InMemoryCompactVector {{ len: {}, width: {}, size: {} }}",
            self.inner.len(),
            self.inner.width(),
            ByteSize(self.inner.size_in_bytes() as u64)
        )
    }
}

impl CompactVectorAccess for InMemoryCompactVector {
    fn access(&self, pos: usize) -> Option<usize> {
        self.inner.access(pos)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn width(&self) -> usize {
        self.inner.width()
    }

    fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }
}

/// File-based compact vector implementation.
///
/// Stores only metadata (len, width, file path, offset) in memory.
/// Reads data from file on each access. Minimal memory but slower access.
///
/// # Design
/// - Caches metadata (len, width) for bounds checking and configuration
/// - Opens file on each access (could be optimized with connection pooling)
/// - Extracts bits on-demand using the same logic as CompactVector
pub struct FileBasedCompactVector {
    /// Path to the file containing the serialized CompactVector
    file_path: PathBuf,
    /// Byte offset in the file where the CompactVector data begins
    offset: u64,
    /// Number of elements in the vector (cached for performance)
    len: usize,
    /// Number of bits per element (cached for performance)
    width: usize,
    /// Size in bytes of the serialized data (for size_in_bytes reporting)
    data_size: usize,
}

impl FileBasedCompactVector {
    /// Create a new file-based compact vector accessor.
    ///
    /// # Arguments
    /// - `file_path`: Path to the file containing the serialized CompactVector
    /// - `offset`: Byte offset where the CompactVector begins
    ///
    /// # Errors
    /// Returns an error if the file cannot be read or the metadata is invalid.
    pub fn new(file_path: impl AsRef<Path>, offset: u64) -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = file_path.as_ref().to_path_buf();
        let file = File::open(&file_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset))?;

        // Read only the metadata without loading the full BitVector
        // CompactVector serialization format:
        //   [BitVector] [len: usize] [width: usize]

        // We need to skip the BitVector and read len/width
        // Unfortunately, we need to deserialize to get the structure
        // For now, load it to extract metadata, but we could optimize this
        let cv = CompactVector::deserialize_from(&mut reader)?;
        let len = cv.len();
        let width = cv.width();
        let data_size = cv.size_in_bytes();

        Ok(Self { file_path, offset, len, width, data_size })
    }

    /// Access element at position by reading from file.
    fn access_from_file(&self, pos: usize) -> Option<usize> {
        if pos >= self.len {
            return None;
        }

        // Open file and seek to offset
        let file = File::open(&self.file_path).ok()?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset)).ok()?;

        // Deserialize the CompactVector and access
        let cv = CompactVector::deserialize_from(&mut reader).ok()?;
        cv.access(pos)
    }
}

impl fmt::Debug for FileBasedCompactVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FileBasedCompactVector {{ file: {:?}, offset: {}, len: {}, width: {}, size: {} }}",
            self.file_path.file_name().unwrap_or_default(),
            self.offset,
            self.len,
            self.width,
            ByteSize(self.data_size as u64)
        )
    }
}

impl CompactVectorAccess for FileBasedCompactVector {
    fn access(&self, pos: usize) -> Option<usize> {
        self.access_from_file(pos)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn width(&self) -> usize {
        self.width
    }

    fn size_in_bytes(&self) -> usize {
        // Report only the metadata size, not the file data
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;

    #[test]
    fn test_in_memory_compact_vector() {
        init();

        let cv = CompactVector::from_slice(&[1, 2, 3, 4, 5]).unwrap();
        let wrapper = InMemoryCompactVector::new(cv);

        assert_eq!(wrapper.len(), 5);
        assert_eq!(wrapper.width(), 3); // 5 requires 3 bits
        assert_eq!(wrapper.access(0), Some(1));
        assert_eq!(wrapper.access(4), Some(5));
        assert_eq!(wrapper.access(5), None);
    }
}
