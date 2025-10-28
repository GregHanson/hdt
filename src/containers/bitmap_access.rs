//! Trait abstraction for bitmap access - allows both in-memory and file-based implementations

use std::fmt::Debug;

use crate::containers::sequence_access::PositionedReader;

/// Trait for accessing bitmaps with rank and select support
///
/// This abstraction allows bitmaps to be either:
/// - In-memory with Rank9Sel indexes (fast O(1) operations)
/// - File-based streaming from disk (slow O(n) operations, minimal memory)
pub trait BitmapAccess: Debug + Send + Sync {
    /// Returns the position of the k-th one bit (0-indexed), or None if there aren't that many
    fn select1(&self, k: usize) -> Option<usize>;

    /// Returns the number of one bits from the 0-th bit to the pos-th bit (exclusive)
    fn rank(&self, pos: usize) -> usize;

    /// Number of bits in the bitmap
    fn len(&self) -> usize;

    /// Whether the bitmap is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of one bits in the bitmap
    fn num_ones(&self) -> usize;

    /// Access a specific bit (returns true if 1, false if 0)
    fn access(&self, pos: usize) -> bool;

    /// Size in bytes (memory footprint)
    fn size_in_bytes(&self) -> usize;
}

/// In-memory bitmap implementation with Rank9Sel indexing (existing Bitmap)
#[derive(Debug, Clone)]
pub struct InMemoryBitmap {
    inner: crate::containers::Bitmap,
}

impl InMemoryBitmap {
    pub fn new(bitmap: crate::containers::Bitmap) -> Self {
        Self { inner: bitmap }
    }

    pub fn into_inner(self) -> crate::containers::Bitmap {
        self.inner
    }

    pub fn inner(&self) -> &crate::containers::Bitmap {
        &self.inner
    }
}

impl BitmapAccess for InMemoryBitmap {
    fn select1(&self, k: usize) -> Option<usize> {
        self.inner.select1(k)
    }

    fn rank(&self, pos: usize) -> usize {
        self.inner.rank(pos)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn num_ones(&self) -> usize {
        self.inner.num_ones()
    }

    fn access(&self, pos: usize) -> bool {
        self.inner.at_last_sibling(pos)
    }

    fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }
}

/// File-based bitmap implementation (streams from disk, no precomputed indexes)
///
/// **WARNING:** This is SLOW compared to InMemoryBitmap:
/// - rank() is O(n) instead of O(1)
/// - select1() is O(n) instead of O(1)
///
/// Use this only when memory is extremely constrained and performance is not critical.
#[derive(Debug)]
pub struct FileBasedBitmap {
    /// File path
    file_path: std::path::PathBuf,
    /// File offset where bitmap data starts (after metadata and CRC8)
    data_offset: u64,
    /// Total number of bits in the bitmap
    num_bits: usize,
    /// Number of 64-bit words in the bitmap
    num_words: usize,
    /// Number of one bits (cached during construction)
    num_ones_cached: usize,
    /// Cached file handle
    file: std::sync::Arc<std::sync::Mutex<PositionedReader>>,
}

impl FileBasedBitmap {
    /// Create a file-based bitmap
    ///
    /// # Arguments
    /// * `file_path` - Path to the HDT file
    /// * `bitmap_offset` - File offset to the START of the bitmap section (including metadata)
    ///
    /// The function will read and validate the metadata, then calculate the actual data offset.
    pub fn new(file_path: std::path::PathBuf, bitmap_offset: u64) -> std::io::Result<Self> {
        use crate::containers::vbyte::read_vbyte;
        use std::io::{Read, Seek, SeekFrom};

        let file = std::fs::File::open(&file_path)?;
        let mut reader = std::io::BufReader::new(file);

        // Seek to the start of the bitmap section
        reader.seek(SeekFrom::Start(bitmap_offset))?;

        let mut metadata_size = 0u64;

        // Read and validate type (1 byte)
        let mut type_buf = [0u8];
        reader.read_exact(&mut type_buf)?;
        metadata_size += 1;
        if type_buf[0] != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported bitmap type: {}, expected 1", type_buf[0]),
            ));
        }

        // Read num_bits (vbyte encoded)
        let (num_bits, vbyte_bytes) = read_vbyte(&mut reader)?;
        metadata_size += vbyte_bytes.len() as u64;

        // Skip CRC8 checksum (1 byte)
        let mut crc_buf = [0u8];
        reader.read_exact(&mut crc_buf)?;
        metadata_size += 1;

        // Calculate the actual data offset
        let data_offset = bitmap_offset + metadata_size;

        // Calculate number of words
        let num_words = if num_bits == 0 { 0 } else { ((num_bits - 1) / 64) + 1 };

        // Count the number of ones by reading all words (we need this for num_ones())
        // This is expensive but only done once during construction
        let num_ones_cached = Self::count_ones(&mut reader, num_bits, num_words)?;

        // Re-open file for the cached reader
        // Re-open file for the cached reader
        let file = std::fs::File::open(&file_path)?;
        let reader = std::io::BufReader::new(file);
        let positioned_reader = PositionedReader::new(reader);

        Ok(Self {
            file_path,
            data_offset,
            num_bits,
            num_words,
            num_ones_cached,
            file: std::sync::Arc::new(std::sync::Mutex::new(positioned_reader)),
        })
    }

    /// Count total number of one bits (helper for construction)
    fn count_ones<R: std::io::Read>(reader: &mut R, num_bits: usize, num_words: usize) -> std::io::Result<usize> {
        let mut count = 0usize;

        if num_words == 0 {
            return Ok(0);
        }

        // Read all full words
        for _ in 0..(num_words - 1) {
            let mut word_buf = [0u8; 8];
            reader.read_exact(&mut word_buf)?;
            let word = u64::from_le_bytes(word_buf);
            count += word.count_ones() as usize;
        }

        // Read last word (byte-aligned)
        let last_word_bits = if num_bits == 0 { 0 } else { ((num_bits - 1) % 64) + 1 };
        let last_word_bytes = (last_word_bits + 7) / 8;
        let mut last_word = 0u64;
        for byte_idx in 0..last_word_bytes {
            let mut byte_buf = [0u8];
            reader.read_exact(&mut byte_buf)?;
            last_word |= (byte_buf[0] as u64) << (byte_idx * 8);
        }

        // Only count bits up to num_bits
        let mask = if last_word_bits == 64 { u64::MAX } else { (1u64 << last_word_bits) - 1 };
        count += (last_word & mask).count_ones() as usize;

        Ok(count)
    }

    /// Read a 64-bit word from the bitmap at the given word index
    fn read_word(&self, word_index: usize) -> std::io::Result<u64> {
        if word_index >= self.num_words {
            return Ok(0);
        }

        let mut reader = self.file.lock().unwrap();

        // Seek to the word position
        let byte_offset = word_index * 8;
        reader.seek_to(self.data_offset + byte_offset as u64)?;

        // Read the word
        if word_index == self.num_words - 1 {
            // Last word is byte-aligned
            let last_word_bits = if self.num_bits == 0 { 0 } else { ((self.num_bits - 1) % 64) + 1 };
            let last_word_bytes = (last_word_bits + 7) / 8;
            let mut word = 0u64;
            for byte_idx in 0..last_word_bytes {
                let mut byte_buf = [0u8];
                reader.read_exact(&mut byte_buf)?;
                word |= (byte_buf[0] as u64) << (byte_idx * 8);
            }
            Ok(word)
        } else {
            let mut word_buf = [0u8; 8];
            reader.read_exact(&mut word_buf)?;
            Ok(u64::from_le_bytes(word_buf))
        }
    }
}

impl BitmapAccess for FileBasedBitmap {
    fn select1(&self, k: usize) -> Option<usize> {
        // O(n) operation - scan words until we find the k-th one bit
        let mut ones_seen = 0usize;

        for word_idx in 0..self.num_words {
            let word = self.read_word(word_idx).ok()?;
            let ones_in_word = word.count_ones() as usize;

            if ones_seen + ones_in_word > k {
                // The k-th one is in this word
                let target_in_word = k - ones_seen;
                let bit_pos = Self::select1_in_word(word, target_in_word)?;
                return Some(word_idx * 64 + bit_pos);
            }

            ones_seen += ones_in_word;
        }

        None
    }

    fn rank(&self, pos: usize) -> usize {
        // O(n) operation - count ones up to position
        if pos >= self.num_bits {
            return self.num_ones_cached;
        }

        let word_index = pos / 64;
        let bit_in_word = pos % 64;
        let mut count = 0usize;

        // Count full words before the target word
        for idx in 0..word_index {
            if let Ok(word) = self.read_word(idx) {
                count += word.count_ones() as usize;
            }
        }

        // Count bits in the target word up to bit_in_word
        if let Ok(word) = self.read_word(word_index) {
            let mask = if bit_in_word == 0 { 0 } else { (1u64 << bit_in_word) - 1 };
            count += (word & mask).count_ones() as usize;
        }

        count
    }

    fn len(&self) -> usize {
        self.num_bits
    }

    fn num_ones(&self) -> usize {
        self.num_ones_cached
    }

    fn access(&self, pos: usize) -> bool {
        if pos >= self.num_bits {
            return false;
        }

        let word_index = pos / 64;
        let bit_in_word = pos % 64;

        if let Ok(word) = self.read_word(word_index) { (word & (1u64 << bit_in_word)) != 0 } else { false }
    }

    fn size_in_bytes(&self) -> usize {
        // Only counting metadata, not the file
        std::mem::size_of::<Self>()
    }
}

impl FileBasedBitmap {
    /// Find the k-th one bit in a 64-bit word (helper function)
    fn select1_in_word(word: u64, k: usize) -> Option<usize> {
        let mut ones_seen = 0usize;
        for bit_pos in 0..64 {
            if (word & (1u64 << bit_pos)) != 0 {
                if ones_seen == k {
                    return Some(bit_pos);
                }
                ones_seen += 1;
            }
        }
        None
    }
}
