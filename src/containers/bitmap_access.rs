//! Trait abstraction for bitmap access - allows in-memory and memory-mapped implementations

use std::fmt::{self, Debug};
use std::sync::Arc;

/// Trait for accessing bitmaps with rank and select support
///
/// This abstraction allows bitmaps to be either:
/// - In-memory with RSNarrow indexes (fast O(1) operations)
/// - Memory-mapped using OS page cache
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

    /// Access a specific bit (returns true if 1, false if 0).
    ///
    /// # Panics
    /// Implementations must panic if `pos >= self.len()`. This method is
    /// called from query iterators that compute their positions by
    /// construction; an out-of-bounds access is always a programming error
    /// in the caller and should fail loudly rather than masquerade as a
    /// false bit.
    fn access(&self, pos: usize) -> bool;

    /// Size in bytes (memory footprint)
    fn size_in_bytes(&self) -> usize;
}

/// In-memory bitmap implementation with RSNarrow indexing (existing Bitmap)
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

/// Memory-mapped bitmap implementation using OS page cache.
///
/// Holds an `Arc<Mmap>` so it can share a single mapping with other
/// components that view the same file (dictionary sections, sequences, ...).
///
/// # Safety contract for callers
///
/// The mapped file must not be modified or truncated for the lifetime of this
/// `MmapBitmap` (and any other accessors built from the same `Arc<Mmap>`).
/// `memmap2::Mmap::map` is `unsafe` for exactly this reason: writing to a
/// mapped file from another process can produce SIGBUS or undefined behavior.
/// HDT files are immutable after generation, so this is fine in practice as
/// long as nothing rewrites the file in place while a query process is live.
///
/// # Performance characteristics
/// - `access()`: O(1) - direct slice indexing
/// - `rank()`:   O(n) - scans words (no rank index)
/// - `select1()`: O(n) - scans words (no select index)
///
/// # Memory usage
/// - Heap: a few `usize` fields plus the `Arc<Mmap>` control block
/// - Bitmap data lives in the OS page cache, demand-paged
pub struct MmapBitmap {
    /// Shared memory map of the underlying file.
    mmap: Arc<memmap2::Mmap>,
    /// Offset to the first byte of bitmap data within the mmap.
    data_offset: usize,
    /// Total number of bits stored in this bitmap.
    num_bits: usize,
    /// Number of 64-bit words covered by the bitmap. The last word may be
    /// byte-aligned (i.e. fewer than 8 bytes if `num_bits % 64 != 0`).
    num_words: usize,
    /// Length of the metadata header (type + vbyte + CRC8) in bytes.
    /// Used by `serialized_size_bytes` so callers can find the next section.
    metadata_size: usize,
    /// Pre-computed number of one bits. Computed at construction time so
    /// `num_ones()` is O(1); the cost is a single sequential scan that the
    /// OS will read ahead efficiently.
    num_ones_cached: usize,
}

impl fmt::Debug for MmapBitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} bits, {} ones, mmaped from file",
            self.num_bits, self.num_ones_cached
        )
    }
}

/// Errors that can occur while parsing a bitmap header out of a memory map.
#[derive(thiserror::Error, Debug)]
pub enum MmapBitmapError {
    #[error("bitmap offset {offset} is past end of mmap (len {len})")]
    OffsetOutOfBounds { offset: u64, len: usize },
    /// The serialized offset is larger than this platform's `usize`. On 32-bit
    /// targets this fires for any offset above `u32::MAX` (~4 GiB). On 64-bit
    /// it cannot fire — `u64::MAX as u64 == usize::MAX as u64`.
    #[error("bitmap offset {offset} exceeds platform usize::MAX ({usize_max}); this cache file requires a 64-bit target")]
    OffsetTooLargeForPlatform { offset: u64, usize_max: u64 },
    #[error("unsupported bitmap type {0}, expected 1")]
    UnsupportedType(u8),
    #[error("truncated bitmap header at offset {0}")]
    Truncated(u64),
    #[error("invalid CRC8 over bitmap header: computed {computed}, stored {stored}")]
    InvalidCrc8 { computed: u8, stored: u8 },
    #[error("bitmap data extends past end of mmap: needs {needed} bytes from offset {offset}, mmap len {len}")]
    DataPastEnd { offset: usize, needed: usize, len: usize },
    #[error("vbyte decode error: {0}")]
    Vbyte(#[from] std::io::Error),
}

impl From<MmapBitmapError> for std::io::Error {
    fn from(e: MmapBitmapError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    }
}

impl MmapBitmap {
    /// Convenience: open the file at `file_path` and mmap it, then parse the
    /// bitmap header at `bitmap_offset`. Most callers should mmap the file
    /// once themselves and call [`Self::from_mmap`] so multiple components
    /// can share one mapping.
    pub fn new(file_path: impl AsRef<std::path::Path>, bitmap_offset: u64) -> std::io::Result<Self> {
        let file = std::fs::File::open(file_path.as_ref())?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Self::from_mmap(Arc::new(mmap), bitmap_offset)
    }

    /// Parse a bitmap header that begins at `bitmap_offset` within the given
    /// shared mmap, validating CRC8 and bounds. The returned `MmapBitmap`
    /// holds a clone of the `Arc`, so the mapping lives as long as any
    /// accessor that references it.
    pub fn from_mmap(mmap: Arc<memmap2::Mmap>, bitmap_offset: u64) -> std::io::Result<Self> {
        use crate::containers::vbyte::read_vbyte;
        use std::io::Cursor;

        // Reject offsets that cannot be addressed on this platform before any
        // truncating cast. On 64-bit `usize::MAX as u64 == u64::MAX` so this
        // never fires; on 32-bit it surfaces a clean error instead of silently
        // wrapping when offsets exceed 4 GiB.
        if bitmap_offset > usize::MAX as u64 {
            return Err(MmapBitmapError::OffsetTooLargeForPlatform {
                offset: bitmap_offset,
                usize_max: usize::MAX as u64,
            }
            .into());
        }

        let mmap_len = mmap.len();
        if (bitmap_offset as usize) > mmap_len {
            return Err(MmapBitmapError::OffsetOutOfBounds { offset: bitmap_offset, len: mmap_len }.into());
        }

        // Parse the header: [type:u8] [num_bits:vbyte] [crc8:u8]. CRC8 covers
        // type byte and the vbyte bytes (matching Bitmap::read).
        let mut cursor = Cursor::new(&mmap[bitmap_offset as usize..]);
        let mut history: Vec<u8> = Vec::with_capacity(8);

        let type_byte = read_one_byte(&mut cursor, bitmap_offset)?;
        history.push(type_byte);
        if type_byte != 1 {
            return Err(MmapBitmapError::UnsupportedType(type_byte).into());
        }

        let (num_bits, vbyte_bytes) =
            read_vbyte(&mut cursor).map_err(|_| MmapBitmapError::Truncated(bitmap_offset))?;
        history.extend_from_slice(&vbyte_bytes);

        let stored_crc = read_one_byte(&mut cursor, bitmap_offset)?;

        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc8.digest();
        digest.update(&history);
        let computed_crc = digest.finalize();
        if computed_crc != stored_crc {
            return Err(MmapBitmapError::InvalidCrc8 { computed: computed_crc, stored: stored_crc }.into());
        }

        let metadata_size = cursor.position() as usize;
        let data_offset = bitmap_offset as usize + metadata_size;
        let num_words = if num_bits == 0 { 0 } else { ((num_bits - 1) / 64) + 1 };

        // Bounds check: every read we will ever perform must lie inside mmap.
        let data_size = bitmap_data_size_bytes(num_bits, num_words);
        if data_offset.saturating_add(data_size) > mmap_len {
            return Err(MmapBitmapError::DataPastEnd { offset: data_offset, needed: data_size, len: mmap_len }.into());
        }

        // Pre-compute num_ones. The constructor reads every byte once, which
        // is the same work the legacy Bitmap::read does. Subsequent calls to
        // num_ones() are then O(1).
        let num_ones_cached =
            count_ones_in_slice(&mmap[data_offset..data_offset + data_size], num_bits);

        Ok(Self { mmap, data_offset, num_bits, num_words, metadata_size, num_ones_cached })
    }

    /// Read a 64-bit word from the bitmap data. Always returns a value (the
    /// constructor verified that all reads up to `num_words` are in bounds);
    /// the last word may have fewer than 64 valid bits.
    fn read_word(&self, word_index: usize) -> u64 {
        debug_assert!(word_index < self.num_words);
        let byte_offset = self.data_offset + word_index * 8;

        if word_index + 1 == self.num_words {
            // Last word is byte-aligned, may be shorter than 8 bytes.
            let last_word_bits = ((self.num_bits - 1) % 64) + 1;
            let last_word_bytes = last_word_bits.div_ceil(8);
            let mut word = 0u64;
            for byte_idx in 0..last_word_bytes {
                word |= (self.mmap[byte_offset + byte_idx] as u64) << (byte_idx * 8);
            }
            word
        } else {
            let bytes: [u8; 8] = self.mmap[byte_offset..byte_offset + 8].try_into().unwrap();
            u64::from_le_bytes(bytes)
        }
    }

    /// Find the k-th one bit in a 64-bit word.
    fn select1_in_word(word: u64, k: usize) -> Option<usize> {
        let mut remaining = word;
        for _ in 0..k {
            // Clear the lowest set bit.
            if remaining == 0 {
                return None;
            }
            remaining &= remaining - 1;
        }
        if remaining == 0 {
            return None;
        }
        Some(remaining.trailing_zeros() as usize)
    }

    /// Calculate the total serialized size of this bitmap on disk, including
    /// the type byte, vbyte length, CRC8, the byte-aligned data and the
    /// trailing CRC32. Used by `new_hybrid_cache` to find the offset of the
    /// section that follows this bitmap in the cache file.
    pub fn serialized_size_bytes(&self) -> usize {
        let data_size = bitmap_data_size_bytes(self.num_bits, self.num_words);
        // metadata + data + CRC32 trailer
        self.metadata_size + data_size + 4
    }

    /// Verify the CRC32 trailer over the bitmap data. This forces all data
    /// pages into memory, so it is *not* called from the constructor. Use it
    /// in long-running services that want to detect on-disk corruption
    /// proactively.
    pub fn verify_crc32(&self) -> Result<(), std::io::Error> {
        let data_size = bitmap_data_size_bytes(self.num_bits, self.num_words);
        let data = &self.mmap[self.data_offset..self.data_offset + data_size];
        let trailer_offset = self.data_offset + data_size;
        if trailer_offset + 4 > self.mmap.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing CRC32 trailer for bitmap",
            ));
        }
        let trailer: [u8; 4] = self.mmap[trailer_offset..trailer_offset + 4].try_into().unwrap();
        let stored = u32::from_le_bytes(trailer);

        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest = crc32.digest();
        digest.update(data);
        let computed = digest.finalize();
        if computed != stored {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid CRC32 over bitmap data: computed {computed}, stored {stored}"),
            ));
        }
        Ok(())
    }
}

impl BitmapAccess for MmapBitmap {
    fn select1(&self, k: usize) -> Option<usize> {
        let mut ones_seen = 0usize;

        for word_idx in 0..self.num_words {
            let word = self.read_word(word_idx);
            let ones_in_word = word.count_ones() as usize;

            if ones_seen + ones_in_word > k {
                let target_in_word = k - ones_seen;
                let bit_pos = Self::select1_in_word(word, target_in_word)?;
                return Some(word_idx * 64 + bit_pos);
            }

            ones_seen += ones_in_word;
        }

        None
    }

    fn rank(&self, pos: usize) -> usize {
        if pos >= self.num_bits {
            return self.num_ones_cached;
        }

        let word_index = pos / 64;
        let bit_in_word = pos % 64;
        let mut count = 0usize;

        for idx in 0..word_index {
            count += self.read_word(idx).count_ones() as usize;
        }

        let word = self.read_word(word_index);
        let mask = if bit_in_word == 0 { 0 } else { (1u64 << bit_in_word) - 1 };
        count += (word & mask).count_ones() as usize;

        count
    }

    fn len(&self) -> usize {
        self.num_bits
    }

    fn num_ones(&self) -> usize {
        self.num_ones_cached
    }

    fn access(&self, pos: usize) -> bool {
        assert!(
            pos < self.num_bits,
            "MmapBitmap::access out of bounds: pos {} >= len {}",
            pos,
            self.num_bits
        );
        let byte_offset = self.data_offset + pos / 8;
        let bit_in_byte = pos % 8;
        (self.mmap[byte_offset] >> bit_in_byte) & 1 == 1
    }

    fn size_in_bytes(&self) -> usize {
        // Heap footprint only: a few words plus the Arc control block.
        // The mmap data lives in the OS page cache and is not counted.
        std::mem::size_of::<Self>()
    }
}

/// Number of bytes the bitmap data occupies on disk for `num_bits` /
/// `num_words`. The last word is byte-aligned.
fn bitmap_data_size_bytes(num_bits: usize, num_words: usize) -> usize {
    if num_words == 0 {
        return 0;
    }
    let full_words = num_words - 1;
    let last_word_bits = ((num_bits - 1) % 64) + 1;
    let last_word_bytes = last_word_bits.div_ceil(8);
    full_words * 8 + last_word_bytes
}

/// Count one bits in a byte-aligned bitmap data slice for `num_bits` total.
fn count_ones_in_slice(data: &[u8], num_bits: usize) -> usize {
    if num_bits == 0 {
        return 0;
    }
    let num_words = ((num_bits - 1) / 64) + 1;
    let mut count = 0usize;

    // Full 8-byte words.
    for word_idx in 0..(num_words - 1) {
        let byte_offset = word_idx * 8;
        let bytes: [u8; 8] = data[byte_offset..byte_offset + 8].try_into().unwrap();
        count += u64::from_le_bytes(bytes).count_ones() as usize;
    }

    // Last (possibly short) word.
    let last_word_bits = ((num_bits - 1) % 64) + 1;
    let last_word_bytes = last_word_bits.div_ceil(8);
    let byte_offset = (num_words - 1) * 8;
    let mut last_word = 0u64;
    for i in 0..last_word_bytes {
        last_word |= (data[byte_offset + i] as u64) << (i * 8);
    }
    let mask = if last_word_bits == 64 { u64::MAX } else { (1u64 << last_word_bits) - 1 };
    count += (last_word & mask).count_ones() as usize;
    count
}

fn read_one_byte<T: AsRef<[u8]>>(cursor: &mut std::io::Cursor<T>, base_offset: u64) -> Result<u8, MmapBitmapError> {
    use std::io::Read;
    let mut buf = [0u8];
    cursor.read_exact(&mut buf).map_err(|_| MmapBitmapError::Truncated(base_offset))?;
    Ok(buf[0])
}
