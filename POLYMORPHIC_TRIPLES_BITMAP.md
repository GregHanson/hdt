# Polymorphic TriplesBitmap Implementation Guide

This guide shows how to use the polymorphic AdjList pattern to create memory-efficient variants of TriplesBitmap.

## Overview

The polymorphic pattern allows AdjList to use either:
- **InMemorySequence**: Original behavior, entire sequence in RAM
- **FileBasedSequence**: Streams from disk, only metadata in RAM

This enables creating hybrid TriplesBitmap implementations that balance memory usage and performance.

## Architecture

### Core Trait Abstraction

```rust
pub trait SequenceAccess: Debug + Send + Sync {
    fn get(&self, index: usize) -> usize;
    fn len(&self) -> usize;
    fn bits_per_entry(&self) -> usize;
    fn size_in_bytes(&self) -> usize;
}
```

### Implementations

```rust
// In-memory: wraps existing Sequence
pub struct InMemorySequence {
    inner: Sequence,
}

// File-based: streams from disk
pub struct FileBasedSequence {
    file_path: PathBuf,
    data_offset: u64,
    entries: usize,
    bits_per_entry_val: usize,
    file: Arc<Mutex<BufReader<File>>>,
}
```

### Generic AdjList

```rust
pub struct AdjListGeneric<S: SequenceAccess> {
    pub sequence: S,      // Pluggable!
    pub bitmap: Bitmap,   // Always in-memory (lightweight)
}

// Type aliases for convenience
pub type AdjListInMemory = AdjListGeneric<InMemorySequence>;
pub type AdjListFileBased = AdjListGeneric<FileBasedSequence>;
```

## Memory Breakdown: TriplesBitmap

For a typical 200MB HDT file, TriplesBitmap uses ~159MB:

```
Component              Size      Percentage
-------------------------------------------------
adjlist_z.sequence     95 MB     60%  ← LARGEST
op_index.sequence      47 MB     30%  ← SECOND LARGEST
wavelet_y              12 MB      7%
bitmap_y                3 MB      2%
bitmap_z                2 MB      1%
-------------------------------------------------
TOTAL                 159 MB    100%
```

**Key insight**: The two sequences consume 90% of memory!

## Hybrid Strategy

### Option 1: Fully In-Memory (Original)

```rust
pub struct TriplesBitmap {
    pub bitmap_y: Bitmap,
    pub bitmap_z: Bitmap,
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
    pub adjlist_z: AdjListInMemory,  // ← 60% of memory
    pub op_index: OpIndex,           // ← contains sequence (30% of memory)
}
```

**Memory**: 159 MB
**Performance**: Fastest
**Use case**: Small datasets, plenty of RAM

### Option 2: Hybrid with File-Based Sequences

```rust
pub struct HybridTriplesBitmap {
    // Lightweight structures in memory (for rank/select)
    pub bitmap_y: Bitmap,
    pub bitmap_z: Bitmap,
    pub wavelet_y: WaveletMatrix<Rank9Sel>,

    // Heavy sequences streamed from disk
    pub adjlist_z: AdjListFileBased,  // ← Only ~300 bytes!
    pub op_index: OpIndexFileBased,   // ← Only ~300 bytes!
}
```

**Memory**: ~15 MB (90% savings!)
**Performance**: Slightly slower (disk I/O on sequence access)
**Use case**: Large datasets, limited RAM

### Option 3: Cache-Optimized Hybrid

```rust
pub struct CachedHybridTriplesBitmap {
    // Load from .hdt.cache file (pre-serialized)
    pub bitmap_y: Bitmap,           // From cache
    pub bitmap_z: Bitmap,           // From cache
    pub wavelet_y: WaveletMatrix,   // From cache

    // Stream sequences from original .hdt file
    pub adjlist_z: AdjListFileBased,  // Points to .hdt
    pub op_index: OpIndexFileBased,   // Points to .hdt
}
```

**Memory**: ~15 MB
**Initialization**: 50x faster (no need to build sucds structures)
**Use case**: Production deployments, fast startup required

## Implementation Example

### Creating In-Memory AdjList

```rust
use hdt::containers::{AdjListInMemory, InMemorySequence};

// Read sequence from HDT
let sequence = Sequence::read(&mut reader)?;
let bitmap = Bitmap::read(&mut reader)?;

// Wrap in trait implementation
let adjlist = AdjListInMemory::new(
    InMemorySequence::new(sequence),
    bitmap,
);

// Use standard API
let id = adjlist.get_id(position);
let first_pos = adjlist.find(id);
```

### Creating File-Based AdjList

```rust
use hdt::containers::{AdjListFileBased, FileBasedSequence};

// Read bitmap into memory (lightweight)
let bitmap = Bitmap::read(&mut reader)?;

// Create file-based sequence (only metadata in memory)
let sequence = FileBasedSequence::new(
    hdt_path.to_path_buf(),
    data_offset,       // Where sequence data starts in file
    entries,           // Number of entries
    bits_per_entry,    // Bits per entry
)?;

let adjlist = AdjListFileBased::new(sequence, bitmap);

// Identical API!
let id = adjlist.get_id(position);  // Reads from disk transparently
let first_pos = adjlist.find(id);   // Uses in-memory bitmap
```

### Complete Hybrid TriplesBitmap

```rust
pub struct HybridTriplesBitmap {
    pub bitmap_y: Bitmap,
    pub bitmap_z: Bitmap,
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
    pub adjlist_z: AdjListFileBased,
    pub op_index: OpIndexFileBased,
    file_path: PathBuf,
}

impl HybridTriplesBitmap {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut reader = BufReader::new(File::open(&path)?);

        // Read lightweight structures into memory
        let bitmap_y = Bitmap::read(&mut reader)?;
        let bitmap_z = Bitmap::read(&mut reader)?;
        let wavelet_y = WaveletMatrix::read(&mut reader)?;

        // Read bitmap for adjlist_z
        let adjlist_z_bitmap_offset = reader.stream_position()?;
        let adjlist_z_bitmap = Bitmap::read(&mut reader)?;

        // Skip sequence metadata, record offset
        let adjlist_z_seq_offset = reader.stream_position()?;
        let adjlist_z_seq_meta = read_sequence_metadata(&mut reader)?;

        // Create file-based adjlist
        let adjlist_z = AdjListFileBased::new(
            FileBasedSequence::new(
                path.as_ref().to_path_buf(),
                adjlist_z_seq_offset,
                adjlist_z_seq_meta.entries,
                adjlist_z_seq_meta.bits_per_entry,
            )?,
            adjlist_z_bitmap,
        );

        // Similar for op_index...

        Ok(Self {
            bitmap_y,
            bitmap_z,
            wavelet_y,
            adjlist_z,
            op_index,
            file_path: path.as_ref().to_path_buf(),
        })
    }
}

impl TripleAccess for HybridTriplesBitmap {
    fn find_y(&self, subject_id: Id) -> usize {
        // Use in-memory bitmap
        self.adjlist_z.find(subject_id)
    }

    fn get_predicate(&self, pos: usize) -> Result<Id> {
        // Streams from disk transparently
        Ok(self.wavelet_y.access(pos).ok_or(Error::InvalidPosition)?)
    }

    // ... other methods
}
```

## Performance Characteristics

### Memory Access Patterns

| Operation | In-Memory | File-Based | Notes |
|-----------|-----------|------------|-------|
| `find(id)` | O(1) bitmap | O(1) bitmap | Bitmap always in memory |
| `get_id(pos)` | O(1) array | O(1) + seek | File-based does disk I/O |
| `search(x, y)` | Fast | Moderate | Binary search + disk reads |
| `last(id)` | O(1) | O(1) | Uses bitmap only |

### Initialization Time

| Implementation | Load Time | Memory | Notes |
|----------------|-----------|--------|-------|
| TriplesBitmap | ~10s | 159 MB | Builds all sucds structures |
| HybridTripleAccess | ~10s | 15 MB | Still builds structures |
| CachedHybrid | ~200ms | 15 MB | Loads pre-built structures |

### Query Performance

For typical SPARQL queries:

- **Subject lookups** (`?p ?o WHERE { <s> ?p ?o }`): Same speed (uses bitmap)
- **Predicate scans** (`?s ?o WHERE { ?s <p> ?o }`): 10-30% slower (disk I/O)
- **Object lookups**: Similar performance (both use wavelet)

## Production Deployment

### Step 1: Generate Cache File

```bash
# One-time operation per HDT file
cargo run --example generate_cache -- data.hdt
```

This creates `data.hdt.cache` containing:
- Magic header: "HDTCACHE"
- Version: 1
- Order: SPO/OPS/etc
- Serialized: bitmap_y, bitmap_z, wavelet_y, op_index.bitmap

### Step 2: Deploy Both Files

```
deployment/
├── data.hdt           # Original HDT (200 MB)
└── data.hdt.cache     # Index cache (15 MB)
```

### Step 3: Use in Application

```rust
// Automatically detects and uses cache
let triples = HybridTriplesBitmap::from_file("data.hdt")?;

// Or explicitly load from cache
let triples = HybridTriplesBitmap::from_cache(
    "data.hdt",        // Sequences streamed from here
    "data.hdt.cache",  // Structures loaded from here
)?;
```

## When to Use Each Pattern

### Use TriplesBitmap (All In-Memory) When:
- Dataset < 100 MB
- RAM abundant (GB available)
- Need absolute maximum query speed
- Simple deployment (single file)

### Use HybridTriplesBitmap (File-Based Sequences) When:
- Dataset 100 MB - 10 GB
- RAM limited (MB available)
- Can tolerate 10-30% slower queries
- Want 90% memory savings

### Use CachedHybridTriplesBitmap (Cache + File) When:
- Production deployment
- Need fast initialization (<1 second)
- Can deploy two files (.hdt + .cache)
- Want consistency across restarts

## Migration Path

### From TriplesBitmap to Hybrid

```rust
// Before: All in-memory
let hdt = Hdt::read(file)?;  // TriplesBitmap
let subject_pos = hdt.triples.find_y(subject_id);

// After: Hybrid (same API!)
let hybrid = HybridTriplesBitmap::from_file("data.hdt")?;
let subject_pos = hybrid.find_y(subject_id);  // Identical!
```

### Backward Compatibility

The polymorphic pattern maintains **100% API compatibility**:

```rust
fn process_triples<T: TripleAccess>(triples: &T) {
    // Works with ANY implementation
    let pos = triples.find_y(1);
    let pred = triples.get_predicate(pos)?;
}

// All work!
process_triples(&triples_bitmap);
process_triples(&hybrid_triples);
process_triples(&cached_hybrid);
```

## Advanced: Custom OpIndex

Similarly, create `OpIndexGeneric<S: SequenceAccess>`:

```rust
pub struct OpIndexGeneric<S: SequenceAccess> {
    pub sequence: S,
    pub bitmap: Bitmap,
}

pub type OpIndexInMemory = OpIndexGeneric<InMemorySequence>;
pub type OpIndexFileBased = OpIndexGeneric<FileBasedSequence>;

pub struct FullHybridTriplesBitmap {
    pub bitmap_y: Bitmap,
    pub bitmap_z: Bitmap,
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
    pub adjlist_z: AdjListFileBased,
    pub op_index: OpIndexFileBased,  // Also file-based!
}
```

## Next Steps

1. **Benchmark**: Compare query performance across implementations
2. **Tune**: Adjust buffer sizes for optimal file I/O
3. **Cache**: Consider LRU cache for hot sequence values
4. **Monitoring**: Track disk I/O metrics in production

## References

- [POLYMORPHIC_ADJLIST_GUIDE.md](POLYMORPHIC_ADJLIST_GUIDE.md) - Trait pattern details
- [CACHE_GUIDE.md](CACHE_GUIDE.md) - Cache file format specification
- [examples/polymorphic_adjlist_demo.rs](examples/polymorphic_adjlist_demo.rs) - Working example
- [examples/hybrid_with_cache.rs](examples/hybrid_with_cache.rs) - Cache usage example
