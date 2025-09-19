# HDT Streaming Implementation Comparison

This document compares the different HDT triple access implementations, with a focus on how the streaming approaches differ from the C++ `hdt-cpp/libhdt/src/triples/BitmapTriples.cpp`.

## Implementation Overview

### 1. Traditional `TriplesBitmap` (Rust)
**File**: `src/triples.rs::TriplesBitmap`

**Approach**: Full in-memory loading, similar to C++ but with Rust optimizations.

**Key Features**:
- Loads all bitmaps, sequences, and indexes into memory
- Builds wavelet matrix for predicate access
- Creates object-to-position index (`OpIndex`)
- Uses `sucds` library for compressed data structures

**Memory Usage**: High - proportional to HDT file size
**Query Performance**: Fast - all data in memory
**Initialization Time**: Moderate - needs to build indexes

### 2. Basic `StreamingTriplesBitmap`
**File**: `src/triples/triples_streaming.rs`

**Approach**: Minimal memory, file-based access.

**Key Features**:
- Stores only file offsets and metadata
- Reads data on-demand from disk
- No pre-calculated indexes
- Reopens file for each query

**Memory Usage**: Minimal - just struct overhead
**Query Performance**: Slow - disk I/O per query
**Initialization Time**: Fast - just parses file structure

### 3. Enhanced `StreamingIndexedTriplesBitmap`
**File**: `src/triples/streaming_indexed.rs`

**Approach**: Hybrid streaming with selective index caching.

**Key Features**:
- Progressive index loading based on memory budget
- Tiered caching strategy
- Falls back to disk when indexes unavailable
- Memory-aware index selection

**Memory Usage**: Configurable - balances memory vs performance
**Query Performance**: Variable - depends on cached indexes
**Initialization Time**: Variable - depends on index building

## Detailed Comparison with C++ BitmapTriples

### C++ Implementation Features (`BitmapTriples.cpp`)

The C++ implementation includes several sophisticated optimizations:

1. **Multiple Index Types**:
   - Predicate index with wavelet matrix
   - Object index for reverse lookups
   - Bitmap sequences for list traversal
   - Specialized iterators for different query patterns

2. **Memory Optimizations**:
   - Bit-level compression (`BitSequence375`)
   - Logarithmic sequence storage (`LogSequence2`)
   - Progressive index complexity
   - Dynamic resizing capabilities

3. **Query Optimizations**:
   - `MiddleWaveletIterator` for predicate-based searches
   - `ObjectIndexIterator` for object-based lookups
   - `BitmapTriplesSearchIterator` for general searches
   - Support for different triple orderings (SPO, OPS, etc.)

4. **Index Generation**:
   - Parallel index construction
   - Memory vs speed trade-offs
   - Progress tracking during index building
   - Support for sorting and reorganization

### How Rust Implementations Differ

#### Traditional `TriplesBitmap` vs C++ BitmapTriples

**Similarities**:
- Both build wavelet matrix for predicates
- Both create object-to-position indexes
- Both use compressed bitmap representations
- Both support rank/select operations

**Differences**:
- **Memory Management**: Rust uses `Box` and `Arc` for owned data vs C++ manual memory management
- **Compression Library**: Rust uses `sucds` vs C++ custom implementations
- **Parallelization**: Rust version has limited parallelization vs C++ threading
- **Iterator Design**: Rust uses trait-based iterators vs C++ class hierarchy

#### Streaming Implementations vs C++ Approach

The streaming implementations represent a fundamentally different philosophy:

**C++ Philosophy**: "Load everything, optimize in memory"
- Assumes sufficient memory for full dataset
- Optimizes for repeated access patterns
- Complex but fast data structures

**Streaming Philosophy**: "Load only what's needed, when it's needed"
- Assumes memory constraints or large datasets
- Optimizes for memory efficiency over speed
- Simple but slower individual operations

### Index Strategy Comparison

| Feature | C++ BitmapTriples | Traditional Rust | Basic Streaming | Indexed Streaming |
|---------|-------------------|------------------|-----------------|-------------------|
| **Subject Index** | ✅ Bitmap + Rank/Select | ✅ Bitmap + Rank/Select | ❌ Disk only | 🔄 Configurable |
| **Predicate Index** | ✅ Wavelet Matrix | ✅ Wavelet Matrix | ❌ Disk only | 🔄 Configurable |
| **Object Index** | ✅ Complex OpIndex | ✅ OpIndex | ❌ Linear scan | 🔄 Configurable |
| **Memory Usage** | High | High | Minimal | Configurable |
| **Initialization** | Slow | Slow | Fast | Variable |
| **Query Speed** | Fast | Fast | Slow | Variable |

## Implementing C++ Optimizations in Streaming Context

### 1. Progressive Index Loading

**C++ Approach**: Build all indexes upfront, choose complexity level
**Streaming Approach**: Build indexes based on available memory budget

```rust
impl StreamingIndexedTriplesBitmap {
    fn build_indexes_progressively(&mut self) -> Result<()> {
        let mut memory_used = 0;

        // Start with most beneficial indexes
        if self.config.build_subject_index &&
           memory_used + self.estimate_subject_size() <= self.config.max_memory {
            self.build_subject_bitmap()?;
            memory_used += self.estimate_subject_size();
        }

        // Add wavelet if memory allows
        if self.config.build_predicate_index &&
           memory_used + self.estimate_wavelet_size() <= self.config.max_memory {
            self.build_predicate_wavelet()?;
        }
        // ... continue with other indexes
    }
}
```

### 2. Tiered Access Patterns

**C++ Approach**: Different iterator classes for different access patterns
**Streaming Approach**: Method calls that use best available index

```rust
impl StreamingIndexedTriplesBitmap {
    pub fn get_predicate(&self, pos_y: usize) -> Result<Id> {
        if let Some(ref wavelet) = self.indexes.predicate_wavelet {
            // Fast path: use cached wavelet
            Ok(wavelet.access(pos_y).unwrap_or(0))
        } else {
            // Slow path: disk access
            self.get_predicate_disk(pos_y)
        }
    }
}
```

### 3. Object Index with Sorting

**C++ Approach**: Pre-sort object positions by predicate
**Streaming Approach**: Sort on-demand if predicate index available

```rust
fn build_object_index(&mut self) -> Result<()> {
    // Build object->positions mapping
    let mut object_positions: HashMap<Id, Vec<usize>> = HashMap::new();
    // ... populate positions ...

    // Sort by predicate if we have the index
    if let Some(ref wavelet) = self.indexes.predicate_wavelet {
        for positions in object_positions.values_mut() {
            positions.sort_by_key(|&pos| {
                self.get_predicate_for_position(pos).unwrap_or(0)
            });
        }
    }
}
```

### 4. Memory-Aware Index Selection

**C++ Approach**: Fixed complexity levels
**Streaming Approach**: Dynamic selection based on available memory

```rust
pub struct IndexConfig {
    pub max_index_memory: usize,
    pub progressive_loading: bool,
    pub priority_order: Vec<IndexType>, // Which indexes to build first
}

enum IndexType {
    SubjectBitmap,     // Usually cheap and beneficial
    PredicateFreq,     // Very cheap, often useful
    PredicateWavelet,  // Expensive but fast for predicate queries
    ObjectIndex,       // Most expensive, for object queries
}
```

## Performance Characteristics

### Query Performance by Pattern

| Query Pattern | C++ BitmapTriples | Traditional Rust | Streaming Basic | Streaming Indexed |
|---------------|-------------------|------------------|-----------------|-------------------|
| **S?? (subject)** | O(log n) | O(log n) | O(n) disk | O(log n) if cached |
| **?P? (predicate)** | O(log n) | O(log n) | O(n) disk | O(log n) if cached |
| **??O (object)** | O(log m) | O(log m) | O(n) disk | O(1) if cached |
| **SPO (specific)** | O(log n) | O(log n) | O(log n) disk | O(log n) |

### Memory vs Performance Trade-offs

```
Memory Usage:
C++ BitmapTriples:     ████████████████████ 100% (full)
Traditional Rust:      ████████████████████ 100% (full)
Streaming Basic:       █ 5% (minimal)
Streaming Indexed:     ████████ 40% (configurable)

Query Speed:
C++ BitmapTriples:     ████████████████████ 100% (fastest)
Traditional Rust:      ████████████████████ 100% (fastest)
Streaming Basic:       ███ 15% (slowest)
Streaming Indexed:     ████████████ 60% (variable)
```

## Use Case Recommendations

### Use Traditional `TriplesBitmap` when:
- ✅ Sufficient memory available (file size < available RAM)
- ✅ Frequent, repeated queries
- ✅ Maximum query performance required
- ✅ Working with moderately sized HDT files

### Use Basic `StreamingTriplesBitmap` when:
- ✅ Memory extremely constrained
- ✅ Infrequent queries
- ✅ Very large HDT files (> available RAM)
- ✅ Many concurrent HDT processes
- ✅ Quick exploration/analysis tasks

### Use `StreamingIndexedTriplesBitmap` when:
- ✅ Moderate memory constraints
- ✅ Mixed query patterns
- ✅ Need to balance memory vs performance
- ✅ Working with multiple HDT files of varying sizes
- ✅ Production systems with memory budgets

## Implementation Roadmap

### Immediate Improvements (similar to C++ optimizations):

1. **Specialized Iterators**:
   ```rust
   pub struct StreamingSubjectIterator<'a> { /* ... */ }
   pub struct StreamingPredicateIterator<'a> { /* ... */ }
   pub struct StreamingObjectIterator<'a> { /* ... */ }
   ```

2. **Compressed Index Representations**:
   - Use `sucds` compressed vectors more extensively
   - Implement delta compression for position lists
   - Add run-length encoding for sparse patterns

3. **Parallel Index Building**:
   ```rust
   use rayon::prelude::*;

   fn build_indexes_parallel(&mut self) -> Result<()> {
       let results: Vec<_> = [
           || self.build_subject_bitmap(),
           || self.build_predicate_wavelet(),
           || self.build_object_index(),
       ].par_iter().map(|f| f()).collect();
       // Handle results...
   }
   ```

4. **Advanced Query Optimization**:
   - Query plan optimization based on available indexes
   - Cost-based index selection
   - Adaptive caching based on query patterns

### Future Enhancements:

1. **Persistent Index Caching**:
   - Save/load pre-built indexes to disk
   - Version compatibility checking
   - Incremental index updates

2. **Multi-level Caching**:
   - L1: Hot data in memory
   - L2: Warm data in compressed form
   - L3: Cold data on disk

3. **Query Pattern Learning**:
   - Analyze query patterns to optimize index selection
   - Adaptive memory allocation
   - Predictive pre-loading

This streaming approach provides a flexible foundation that can incorporate the best aspects of the C++ implementation while offering memory efficiency advantages that the C++ version lacks.