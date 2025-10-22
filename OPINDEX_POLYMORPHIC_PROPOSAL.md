# Polymorphic OpIndex Implementation Proposal

## Goal
Make `OpIndex.sequence` polymorphic to support both in-memory (`CompactVector`) and file-based access, following the same pattern as `AdjList.sequence`.

## Proposed Architecture

### 1. Create CompactVectorAccess Trait

```rust
// src/containers/compact_vector_access.rs

use std::fmt::Debug;

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

/// In-memory compact vector (existing CompactVector from sucds)
#[derive(Debug)]
pub struct InMemoryCompactVector {
    inner: sucds::int_vectors::CompactVector,
}

impl InMemoryCompactVector {
    pub fn new(cv: sucds::int_vectors::CompactVector) -> Self {
        Self { inner: cv }
    }

    pub fn inner(&self) -> &sucds::int_vectors::CompactVector {
        &self.inner
    }

    pub fn into_inner(self) -> sucds::int_vectors::CompactVector {
        self.inner
    }
}

impl CompactVectorAccess for InMemoryCompactVector {
    fn get(&self, index: usize) -> usize {
        self.inner.access(index).unwrap()
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

/// File-based compact vector (streams from disk)
#[derive(Debug)]
pub struct FileBasedCompactVector {
    file_path: std::path::PathBuf,
    data_offset: u64,
    entries: usize,
    width_val: usize,
    file: std::sync::Arc<std::sync::Mutex<std::io::BufReader<std::fs::File>>>,
}

impl FileBasedCompactVector {
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

        // Read enough bytes
        let bytes_needed = ((self.width_val + bit_in_byte + 7) / 8).min(16);
        let mut buffer = vec![0u8; bytes_needed];
        reader.read_exact(&mut buffer)?;

        // Extract bits (same logic as FileBasedSequence)
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
        // Only metadata, not the file
        std::mem::size_of::<Self>()
    }
}
```

### 2. Create Generic OpIndex

```rust
// src/triples.rs (updated OpIndex)

/// Generic OpIndex with pluggable compact vector implementation
pub struct OpIndexGeneric<CV: CompactVectorAccess> {
    /// Compact integer vector of object positions (generic)
    pub sequence: CV,
    /// Bitmap with a one bit for every new object
    pub bitmap: Bitmap,
}

/// Type alias for traditional in-memory OpIndex
pub type OpIndex = OpIndexGeneric<InMemoryCompactVector>;

/// Type alias for file-based OpIndex
pub type OpIndexFileBased = OpIndexGeneric<FileBasedCompactVector>;

impl<CV: CompactVectorAccess> OpIndexGeneric<CV> {
    pub fn new(sequence: CV, bitmap: Bitmap) -> Self {
        Self { sequence, bitmap }
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.bitmap.size_in_bytes()
    }

    /// Find the first position in the OP index of the given object ID.
    pub fn find(&self, o: Id) -> usize {
        self.bitmap.select1(o - 1).unwrap() as usize
    }

    /// Find the last position in the object index of the given object ID.
    pub fn last(&self, o: Id) -> usize {
        self.bitmap.select1(o).map_or_else(|| self.bitmap.len() - 1, |index| index as usize - 1)
    }

    /// Get value at index
    pub fn get(&self, index: usize) -> usize {
        self.sequence.get(index)
    }
}

impl<CV: CompactVectorAccess> fmt::Debug for OpIndexGeneric<CV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "total size {} {{", ByteSize(self.size_in_bytes() as u64))?;
        writeln!(
            f,
            "    sequence: {} with {} bits,",
            ByteSize(self.sequence.size_in_bytes() as u64),
            self.sequence.width()
        )?;
        write!(f, "    bitmap: {:#?}\n}}", self.bitmap)
    }
}
```

### 3. Update TriplesBitmapGeneric

Make `TriplesBitmapGeneric` generic over TWO types:

```rust
/// Generic TriplesBitmap with pluggable sequence implementations
pub struct TriplesBitmapGeneric<S: SequenceAccess, CV: CompactVectorAccess> {
    order: Order,
    pub bitmap_y: Bitmap,
    pub adjlist_z: AdjListGeneric<S>,
    pub op_index: OpIndexGeneric<CV>,
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
}

/// Traditional all-in-memory TriplesBitmap
pub type TriplesBitmap = TriplesBitmapGeneric<InMemorySequence, InMemoryCompactVector>;

/// Hybrid: file-based sequences, in-memory bitmaps/wavelet
pub type HybridTriplesBitmap = TriplesBitmapGeneric<FileBasedSequence, FileBasedCompactVector>;

/// Mixed: in-memory adjlist, file-based opindex (or vice versa)
pub type MixedTriplesBitmap1 = TriplesBitmapGeneric<InMemorySequence, FileBasedCompactVector>;
pub type MixedTriplesBitmap2 = TriplesBitmapGeneric<FileBasedSequence, InMemoryCompactVector>;
```

### 4. Usage Examples

```rust
// Traditional (existing code unchanged)
let triples = TriplesBitmap::from_triples(&triple_ids);
// adjlist_z: InMemorySequence
// op_index: InMemoryCompactVector

// Hybrid (90% memory savings)
let hybrid = HybridTriplesBitmap::from_cache("data.hdt", "data.hdt.cache")?;
// adjlist_z: FileBasedSequence
// op_index: FileBasedCompactVector

// Mixed (customize per use case)
let mixed = TriplesBitmapGeneric::<InMemorySequence, FileBasedCompactVector>::new(...);
// adjlist_z: InMemory (fast access)
// op_index: FileBased (less frequently accessed)
```

## Migration Path

### Phase 1: Create Trait (minimal changes)

1. Create `src/containers/compact_vector_access.rs`
2. Add `InMemoryCompactVector` wrapper
3. No changes to existing code yet

### Phase 2: Make OpIndex Generic

1. Change `OpIndex` to `OpIndexGeneric<CV>`
2. Add type alias: `pub type OpIndex = OpIndexGeneric<InMemoryCompactVector>`
3. Update `OpIndex::new()` calls to wrap `CompactVector` in `InMemoryCompactVector`

### Phase 3: Make TriplesBitmap Generic Over Both

1. Change `TriplesBitmapGeneric<S>` to `TriplesBitmapGeneric<S, CV>`
2. Update type alias: `pub type TriplesBitmap = TriplesBitmapGeneric<InMemorySequence, InMemoryCompactVector>`
3. All existing code continues to work

### Phase 4: Add File-Based Implementations

1. Implement `FileBasedCompactVector`
2. Create `HybridTriplesBitmap` type alias
3. Add builder methods for hybrid variants

## Memory Impact

For a typical 200MB HDT file:

| Component | Traditional | Hybrid | Savings |
|-----------|-------------|--------|---------|
| adjlist_z.sequence | 95 MB | ~300 bytes | 95 MB |
| op_index.sequence | 47 MB | ~300 bytes | 47 MB |
| wavelet_y | 12 MB | 12 MB | 0 |
| bitmap_y | 3 MB | 3 MB | 0 |
| bitmap_z | 2 MB | 2 MB | 0 |
| **Total** | **159 MB** | **~17 MB** | **~142 MB (89%)** |

## Why NOT Option<CompactVector>?

```rust
// ❌ Option-based approach (NOT recommended)
pub struct OpIndex {
    pub sequence: Option<CompactVector>,  // What does None mean?
    pub bitmap: Bitmap,
    file_path: Option<PathBuf>,            // Need to add this
    data_offset: Option<u64>,              // And this
    entries: Option<usize>,                // And this
    width: Option<usize>,                  // And this
}

impl OpIndex {
    pub fn get(&self, index: usize) -> usize {
        // Runtime checks on EVERY access
        if let Some(ref seq) = self.sequence {
            seq.access(index).unwrap()
        } else {
            // Need to read from file
            // But where's the file handle? Need Arc<Mutex<...>> too
            self.read_from_file(index)?
        }
    }
}

// Problems:
// 1. Every access has runtime overhead
// 2. Unclear what None means
// 3. Need to add many Option fields for file access
// 4. Error handling is awkward
// 5. Inconsistent with adjlist_z pattern
```

## Conclusion

**Use the trait-based approach** because:

1. ✅ Consistent with `SequenceAccess` pattern
2. ✅ Type-safe and zero-cost
3. ✅ Clear and explicit
4. ✅ Easy to extend
5. ✅ Better performance (no runtime checks)

The Option approach would save some boilerplate initially, but creates technical debt and runtime overhead that's not worth it.
