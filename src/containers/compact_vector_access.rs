//! Access trait and implementations for CompactVector-like structures.
//!
//! This module provides trait-based abstraction over different storage strategies
//! for compact integer vectors (bit-packed arrays), similar to BitmapAccess but for
//! sucds::CompactVector structures.

use bytesize::ByteSize;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use sucds::Serializable;
use sucds::int_vectors::{Access, CompactVector};

use crate::containers::sequence_access::USIZE_BITS;

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
/// Stores only metadata (len, width) in memory and uses PositionedReader for efficient file access.
/// Reads data from file on each access. Minimal memory but slower access.
///
/// # Design
/// - Caches metadata (len, width, bitvector_len) for bounds checking
/// - Uses PositionedReader to maintain file handle and position, avoiding repeated open/seek
/// - Reads only the necessary 1-2 words (8-16 bytes) from file on each access
/// - Extracts bits on-demand using the same logic as CompactVector::get_int()
/// - File format: [Vec<usize> words][usize bitvec_len][usize cv_len][usize width]
pub struct FileBasedCompactVector {
    /// Byte offset in the file where the BitVector words begin
    bitvector_offset: u64,
    /// Number of bits in the BitVector (for bounds checking)
    bitvector_len: usize,
    /// Number of elements in the CompactVector (cached for performance)
    len: usize,
    /// Number of bits per element (cached for performance)
    width: usize,
    /// Cached file handle with position tracking
    file: std::sync::Arc<std::sync::Mutex<crate::containers::sequence_access::PositionedReader>>,
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
        use std::mem::size_of;

        let file_path = file_path.as_ref();
        let file = File::open(file_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset))?;

        // CompactVector serialization format:
        //   [Vec<usize> words] [usize bitvec_len] [usize cv_len] [usize width]

        // Read Vec<usize> length (the word count)
        let bitvector_offset = offset + size_of::<usize>() as u64; // After the Vec length
        let mut buf = [0u8; size_of::<usize>()];
        reader.read_exact(&mut buf)?;
        let bitvector_word_count = usize::from_le_bytes(buf);

        // Skip the words themselves
        reader.seek(SeekFrom::Current((bitvector_word_count * size_of::<usize>()) as i64))?;

        // Read bitvector len
        reader.read_exact(&mut buf)?;
        let bitvector_len = usize::from_le_bytes(buf);

        // Read CompactVector len
        reader.read_exact(&mut buf)?;
        let len = usize::from_le_bytes(buf);

        // Read width
        reader.read_exact(&mut buf)?;
        let width = usize::from_le_bytes(buf);

        // Re-open file for the cached reader
        let file = File::open(file_path)?;
        let buf_reader = BufReader::new(file);
        let positioned_reader = crate::containers::sequence_access::PositionedReader::new(buf_reader);
        let file = std::sync::Arc::new(std::sync::Mutex::new(positioned_reader));

        Ok(Self { bitvector_offset, bitvector_len, len, width, file })
    }

    /// Access element at position by reading only the necessary words from file.
    ///
    /// This implements the same logic as BitVector::get_bits() but reads from file.
    /// Uses PositionedReader to avoid repeated open/seek operations.
    fn access_from_file(&self, pos: usize) -> Option<usize> {
        use std::mem::size_of;

        if pos >= self.len {
            return None;
        }

        // Calculate bit position in the BitVector
        let bit_pos = pos * self.width;
        let bit_len = self.width;

        // Bounds check
        if bit_len > USIZE_BITS || self.bitvector_len < bit_pos + bit_len {
            return None;
        }

        if bit_len == 0 {
            return Some(0);
        }

        // Calculate which word(s) we need to read
        let block = bit_pos / USIZE_BITS;
        let shift = bit_pos % USIZE_BITS;

        // Lock the file reader and seek to the required word
        let mut file = self.file.lock().ok()?;
        let word_offset = self.bitvector_offset + (block * size_of::<usize>()) as u64;
        file.seek_to(word_offset).ok()?;

        // Read the first word
        let mut buf = [0u8; size_of::<usize>()];
        file.read_exact(&mut buf).ok()?;
        let word1 = usize::from_le_bytes(buf);

        // Calculate mask for extracting bits
        let mask = if bit_len < USIZE_BITS { (1 << bit_len) - 1 } else { usize::MAX };

        // Extract bits
        let bits = if shift + bit_len <= USIZE_BITS {
            // All bits are in the first word
            (word1 >> shift) & mask
        } else {
            // Bits span two words - read the second word
            file.read_exact(&mut buf).ok()?;
            let word2 = usize::from_le_bytes(buf);
            (word1 >> shift) | ((word2 << (USIZE_BITS - shift)) & mask)
        };

        Some(bits)
    }
}

impl fmt::Debug for FileBasedCompactVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, compact vector read directly from file", ByteSize(std::mem::size_of::<Self>() as u64))
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

    #[test]
    fn test_file_based_compact_vector() {
        use std::io::Write;
        use sucds::Serializable;

        init();

        // Create a CompactVector with test data
        let test_data: Vec<usize> = (0..100).collect();
        let cv = CompactVector::from_slice(&test_data).unwrap();

        // Write to a temporary file
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_compact_vector.bin");
        let mut file = std::fs::File::create(&temp_file).unwrap();
        cv.serialize_into(&mut file).unwrap();
        file.flush().unwrap();
        drop(file);

        // Create file-based accessor
        let fb_cv = FileBasedCompactVector::new(&temp_file, 0).unwrap();

        // Verify metadata
        assert_eq!(fb_cv.len(), 100);
        assert_eq!(fb_cv.width(), 7); // 99 requires 7 bits

        // Test random access
        assert_eq!(fb_cv.access(0), Some(0));
        assert_eq!(fb_cv.access(50), Some(50));
        assert_eq!(fb_cv.access(99), Some(99));
        assert_eq!(fb_cv.access(100), None); // Out of bounds

        // Verify all values match
        for i in 0..100 {
            assert_eq!(fb_cv.access(i), Some(i), "Mismatch at position {}", i);
        }

        // Clean up
        std::fs::remove_file(&temp_file).ok();
    }
}
