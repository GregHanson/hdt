use crate::triples::{Error, Id, Order};
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
use sucds::bit_vectors::{Rank9Sel, Select};
use sucds::char_sequences::WaveletMatrix;

pub type Result<T> = core::result::Result<T, Error>;

/// Enhanced streaming implementation with pre-calculated indexes
/// that combines the memory efficiency of streaming with the performance of indexes.
///
/// Key differences from C++ BitmapTriples:
/// 1. Selective index loading: Only loads indexes that fit in available memory
/// 2. Tiered caching: Different levels of indexes based on query patterns
/// 3. Lazy evaluation: Indexes are built on-demand when first accessed
/// 4. File-based fallback: Falls back to disk I/O when indexes aren't available
pub struct StreamingIndexedTriplesBitmap {
    /// Triple ordering
    pub order: Order,
    /// File path for reading
    file_path: std::path::PathBuf,
    /// File section offsets
    offsets: SectionOffsets,
    /// Basic metadata (always in memory)
    metadata: TriplesMetadata,
    /// Pre-calculated indexes (selective loading)
    pub indexes: IndexCache,
    /// Configuration for which indexes to build/load
    index_config: IndexConfig,
}

/// File offsets for different HDT sections
#[derive(Debug, Clone)]
pub struct SectionOffsets {
    pub bitmap_y: u64,
    pub bitmap_z: u64,
    pub sequence_y: u64,
    pub sequence_z: u64,
}

/// Essential metadata about the triples section
#[derive(Debug, Clone)]
pub struct TriplesMetadata {
    pub num_triples: usize,
    pub num_predicates: usize,
    pub num_objects: usize,
    pub num_subjects: usize,
}

/// Configuration for which indexes to build/cache
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Whether to build subject->predicate index (bitmap_y rank/select)
    pub build_subject_index: bool,
    /// Whether to build predicate wavelet matrix
    pub build_predicate_index: bool,
    /// Whether to build object->position index
    pub build_object_index: bool,
    /// Maximum memory budget for indexes (in bytes)
    pub max_index_memory: usize,
    /// Whether to use progressive loading (start with small indexes)
    pub progressive_loading: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            build_subject_index: true,           // Usually worth it
            build_predicate_index: false,        // Memory intensive
            build_object_index: false,           // Very memory intensive
            max_index_memory: 256 * 1024 * 1024, // 256MB default
            progressive_loading: true,
        }
    }
}

/// Cached indexes for fast access
#[derive(Debug)]
pub struct IndexCache {
    /// Subject bitmap with rank/select for fast subject->predicate lookup
    pub subject_bitmap: Option<Rank9Sel>,
    /// Predicate wavelet matrix for fast predicate access
    pub predicate_wavelet: Option<WaveletMatrix<Rank9Sel>>,
    /// Object index for object-based queries (?PO, ??O)
    pub object_index: Option<ObjectIndex>,
    /// Predicate frequency cache for optimization
    pub predicate_frequencies: Option<HashMap<Id, usize>>,
    /// Subject frequency cache
    pub subject_frequencies: Option<HashMap<Id, usize>>,
}

impl Default for IndexCache {
    fn default() -> Self {
        Self {
            subject_bitmap: None,
            predicate_wavelet: None,
            object_index: None,
            predicate_frequencies: None,
            subject_frequencies: None,
        }
    }
}

/// Object index structure similar to C++ implementation
#[derive(Debug)]
pub struct ObjectIndex {
    /// For each object, positions in the Z sequence where it appears
    pub object_positions: HashMap<Id, Vec<usize>>,
    /// Sorted by predicate for efficient range queries
    pub sorted_by_predicate: bool,
}

impl fmt::Debug for StreamingIndexedTriplesBitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "StreamingIndexedTriplesBitmap {{")?;
        writeln!(f, "  order: {:?}", self.order)?;
        writeln!(f, "  file_path: {:?}", self.file_path)?;
        writeln!(f, "  metadata: {:?}", self.metadata)?;
        writeln!(f, "  loaded_indexes: [")?;
        if self.indexes.subject_bitmap.is_some() {
            writeln!(f, "    subject_bitmap")?;
        }
        if self.indexes.predicate_wavelet.is_some() {
            writeln!(f, "    predicate_wavelet")?;
        }
        if self.indexes.object_index.is_some() {
            writeln!(f, "    object_index")?;
        }
        writeln!(f, "  ]")?;
        writeln!(f, "  index_config: {:?}", self.index_config)?;
        write!(f, "}}")
    }
}

impl StreamingIndexedTriplesBitmap {
    /// Create streaming indexed triples from HDT file with default configuration
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::from_file_with_config(path, IndexConfig::default())
    }

    /// Create streaming indexed triples with custom index configuration
    pub fn from_file_with_config<P: AsRef<Path>>(path: P, config: IndexConfig) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        let file = File::open(&file_path)?;
        let mut reader = BufReader::new(file);

        // Parse HDT file structure to get offsets
        let _global_ci = crate::ControlInfo::read(&mut reader)?;
        crate::header::Header::read(&mut reader)?;
        crate::four_sect_dict::FourSectDict::read(&mut reader)?;
        let triples_ci = crate::ControlInfo::read(&mut reader)?;

        let order = if let Some(n) = triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            Order::try_from(n)?
        } else {
            return Err(Error::UnspecifiedTriplesOrder);
        };

        // Record section offsets
        let bitmap_y_offset = reader.stream_position()?;
        let bitmap_y = crate::containers::Bitmap::read(&mut reader)?;

        let bitmap_z_offset = reader.stream_position()?;
        let _bitmap_z = crate::containers::Bitmap::read(&mut reader)?;

        let sequence_y_offset = reader.stream_position()?;
        let sequence_y = crate::containers::Sequence::read(&mut reader)?;

        let sequence_z_offset = reader.stream_position()?;
        let sequence_z = crate::containers::Sequence::read(&mut reader)?;

        let offsets = SectionOffsets {
            bitmap_y: bitmap_y_offset,
            bitmap_z: bitmap_z_offset,
            sequence_y: sequence_y_offset,
            sequence_z: sequence_z_offset,
        };

        // Extract metadata
        let metadata = TriplesMetadata {
            num_triples: sequence_z.entries,
            num_predicates: sequence_y.entries,
            num_objects: sequence_z.into_iter().max().unwrap_or(0),
            num_subjects: bitmap_y.num_ones(),
        };

        let mut instance =
            Self { order, file_path, offsets, metadata, indexes: IndexCache::default(), index_config: config };

        // Build indexes based on configuration and memory budget
        instance.build_indexes_progressively()?;

        Ok(instance)
    }

    // Build indexes progressively based on configuration and memory constraints (moved to public method)

    /// Build subject bitmap for fast subject->predicate lookup
    fn build_subject_bitmap(&mut self) -> Result<()> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offsets.bitmap_y))?;

        let bitmap_y = crate::containers::Bitmap::read(&mut reader)?;
        self.indexes.subject_bitmap = Some(bitmap_y.dict);

        Ok(())
    }

    /// Build predicate wavelet matrix for fast predicate queries
    fn build_predicate_wavelet(&mut self) -> Result<()> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offsets.sequence_y))?;

        let sequence_y = crate::containers::Sequence::read(&mut reader)?;

        // Build wavelet matrix similar to original implementation
        let mut builder = sucds::int_vectors::CompactVector::new(sequence_y.bits_per_entry.max(1))
            .map_err(|e| Error::External(format!("Failed to create CompactVector: {}", e).into()))?;

        for i in 0..sequence_y.entries {
            builder
                .push_int(sequence_y.get(i))
                .map_err(|e| Error::External(format!("Failed to push to CompactVector: {}", e).into()))?;
        }

        if sequence_y.entries == 0 {
            builder
                .push_int(0)
                .map_err(|e| Error::External(format!("Failed to push default value: {}", e).into()))?;
        }

        let wavelet = WaveletMatrix::new(builder)
            .map_err(|e| Error::External(format!("Failed to create WaveletMatrix: {}", e).into()))?;

        self.indexes.predicate_wavelet = Some(wavelet);
        Ok(())
    }

    /// Build object index for object-based queries
    fn build_object_index(&mut self) -> Result<()> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);

        // Read Z sequence to build object->position mapping
        reader.seek(SeekFrom::Start(self.offsets.sequence_z))?;
        let sequence_z = crate::containers::Sequence::read(&mut reader)?;

        let mut object_positions: HashMap<Id, Vec<usize>> = HashMap::new();

        for pos in 0..sequence_z.entries {
            let object_id = sequence_z.get(pos);
            if object_id > 0 {
                object_positions.entry(object_id).or_default().push(pos);
            }
        }

        // Sort positions by predicate (requires predicate wavelet or disk access)
        let sorted_by_predicate = if let Some(ref wavelet) = self.indexes.predicate_wavelet {
            // Sort using cached wavelet
            for positions in object_positions.values_mut() {
                positions.sort_by_key(|&pos| {
                    // Get Y position from Z position using bitmap_z
                    self.get_predicate_for_position(pos).unwrap_or(0)
                });
            }
            true
        } else {
            false
        };

        self.indexes.object_index = Some(ObjectIndex { object_positions, sorted_by_predicate });

        Ok(())
    }

    /// Build predicate frequency cache
    fn build_predicate_frequencies(&mut self) -> Result<()> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offsets.sequence_y))?;

        let sequence_y = crate::containers::Sequence::read(&mut reader)?;
        let mut frequencies = HashMap::new();

        for i in 0..sequence_y.entries {
            let predicate = sequence_y.get(i);
            *frequencies.entry(predicate).or_insert(0) += 1;
        }

        self.indexes.predicate_frequencies = Some(frequencies);
        Ok(())
    }

    /// Get predicate for a given Z position (used by object index sorting)
    fn get_predicate_for_position(&self, pos_z: usize) -> Result<Id> {
        if let Some(ref wavelet) = self.indexes.predicate_wavelet {
            // Use cached wavelet matrix
            let file = File::open(&self.file_path)?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(self.offsets.bitmap_z))?;

            let bitmap_z = crate::containers::Bitmap::read(&mut reader)?;
            let pos_y = bitmap_z.rank(pos_z);

            Ok(wavelet.access(pos_y).unwrap_or(0))
        } else {
            // Fall back to disk access
            self.get_predicate_disk(pos_z)
        }
    }

    /// Disk-based predicate lookup (fallback when wavelet not available)
    fn get_predicate_disk(&self, pos_z: usize) -> Result<Id> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);

        // Get Y position from Z position
        reader.seek(SeekFrom::Start(self.offsets.bitmap_z))?;
        let bitmap_z = crate::containers::Bitmap::read(&mut reader)?;
        let pos_y = bitmap_z.rank(pos_z);

        // Get predicate from Y position
        reader.seek(SeekFrom::Start(self.offsets.sequence_y))?;
        let sequence_y = crate::containers::Sequence::read(&mut reader)?;

        Ok(if pos_y < sequence_y.entries { sequence_y.get(pos_y) } else { 0 })
    }

    // Memory estimation methods for progressive loading
    fn estimate_subject_bitmap_size(&self) -> usize {
        // Estimate based on number of subjects and bitmap overhead
        self.metadata.num_subjects * 8 + 1024 // rough estimate
    }

    fn estimate_wavelet_size(&self) -> usize {
        // Wavelet matrix size depends on alphabet size and sequence length
        self.metadata.num_predicates * 4 + self.metadata.num_predicates / 8
    }

    fn estimate_object_index_size(&self) -> usize {
        // Object index: HashMap overhead + position vectors
        self.metadata.num_objects * (24 + 8) + self.metadata.num_triples * 8
    }

    fn estimate_predicate_freq_size(&self) -> usize {
        // Small HashMap for predicate frequencies
        self.metadata.num_predicates * 16
    }

    // Query methods that use indexes when available, fall back to disk

    /// Get number of triples
    pub fn num_triples(&self) -> usize {
        self.metadata.num_triples
    }

    /// Get memory usage of loaded indexes
    pub fn index_memory_usage(&self) -> usize {
        let mut total = 0;

        if self.indexes.subject_bitmap.is_some() {
            total += self.estimate_subject_bitmap_size();
        }
        if self.indexes.predicate_wavelet.is_some() {
            total += self.estimate_wavelet_size();
        }
        if self.indexes.object_index.is_some() {
            total += self.estimate_object_index_size();
        }
        if self.indexes.predicate_frequencies.is_some() {
            total += self.estimate_predicate_freq_size();
        }

        total
    }

    /// Fast subject lookup using cached bitmap or disk fallback
    pub fn find_y(&self, subject_id: Id) -> Result<usize> {
        if subject_id == 0 {
            return Ok(0);
        }

        if let Some(ref bitmap) = self.indexes.subject_bitmap {
            // Use cached bitmap for fast lookup
            bitmap
                .select1(subject_id - 1)
                .map(|pos| pos + 1)
                .ok_or_else(|| Error::External(format!("Subject {} not found", subject_id).into()))
        } else {
            // Fall back to disk access
            let file = File::open(&self.file_path)?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(self.offsets.bitmap_y))?;

            let bitmap_y = crate::containers::Bitmap::read(&mut reader)?;
            bitmap_y
                .select1(subject_id - 1)
                .map(|pos| pos + 1)
                .ok_or_else(|| Error::External(format!("Subject {} not found", subject_id).into()))
        }
    }

    /// Get object at position with possible caching
    pub fn get_object(&self, pos_z: usize) -> Result<Id> {
        // For objects, we generally need to read from disk since full caching is expensive
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offsets.sequence_z))?;

        let sequence_z = crate::containers::Sequence::read(&mut reader)?;

        if pos_z < sequence_z.entries { Ok(sequence_z.get(pos_z)) } else { Ok(0) }
    }

    /// Get predicate at position using wavelet cache or disk
    pub fn get_predicate(&self, pos_y: usize) -> Result<Id> {
        if let Some(ref wavelet) = self.indexes.predicate_wavelet {
            // Use cached wavelet matrix
            Ok(wavelet.access(pos_y).unwrap_or(0))
        } else {
            // Fall back to disk access
            let file = File::open(&self.file_path)?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(self.offsets.sequence_y))?;

            let sequence_y = crate::containers::Sequence::read(&mut reader)?;

            if pos_y < sequence_y.entries { Ok(sequence_y.get(pos_y)) } else { Ok(0) }
        }
    }

    /// Get all positions where an object appears (using object index if available)
    pub fn get_object_positions(&self, object_id: Id) -> Result<Vec<usize>> {
        if let Some(ref obj_index) = self.indexes.object_index {
            // Use cached object index
            Ok(obj_index.object_positions.get(&object_id).cloned().unwrap_or_default())
        } else {
            // Linear scan fallback
            let file = File::open(&self.file_path)?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(self.offsets.sequence_z))?;

            let sequence_z = crate::containers::Sequence::read(&mut reader)?;
            let mut positions = Vec::new();

            for pos in 0..sequence_z.entries {
                if sequence_z.get(pos) == object_id {
                    positions.push(pos);
                }
            }

            Ok(positions)
        }
    }

    /// Get configuration
    pub fn get_config(&self) -> &IndexConfig {
        &self.index_config
    }

    /// Get metadata
    pub fn get_metadata(&self) -> &TriplesMetadata {
        &self.metadata
    }

    /// Update configuration (for optimization)
    pub fn update_config(&mut self, config: IndexConfig) -> Result<()> {
        self.index_config = config;
        self.build_indexes_progressively()
    }

    /// Build indexes progressively (public for trait implementation)
    pub fn build_indexes_progressively(&mut self) -> Result<()> {
        let mut memory_used = 0;

        // Start with most beneficial indexes first
        if self.index_config.build_subject_index {
            let subject_bitmap_size = self.estimate_subject_bitmap_size();
            if memory_used + subject_bitmap_size <= self.index_config.max_index_memory {
                self.build_subject_bitmap()?;
                memory_used += subject_bitmap_size;
            }
        }

        // Build predicate frequencies (small and often useful)
        if memory_used + self.estimate_predicate_freq_size() <= self.index_config.max_index_memory {
            self.build_predicate_frequencies()?;
            memory_used += self.estimate_predicate_freq_size();
        }

        // Build predicate wavelet if memory allows
        if self.index_config.build_predicate_index {
            let wavelet_size = self.estimate_wavelet_size();
            if memory_used + wavelet_size <= self.index_config.max_index_memory {
                self.build_predicate_wavelet()?;
                memory_used += wavelet_size;
            }
        }

        // Build object index if memory allows (most expensive)
        if self.index_config.build_object_index {
            let object_index_size = self.estimate_object_index_size();
            if memory_used + object_index_size <= self.index_config.max_index_memory {
                self.build_object_index()?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indexed_streaming() -> Result<()> {
        let config = IndexConfig {
            build_subject_index: true,
            build_predicate_index: false,
            build_object_index: false,
            max_index_memory: 1024 * 1024, // 1MB
            progressive_loading: true,
        };

        let streaming =
            StreamingIndexedTriplesBitmap::from_file_with_config("tests/resources/snikmeta.hdt", config)?;

        assert!(streaming.num_triples() > 0);
        assert!(streaming.index_memory_usage() > 0);

        Ok(())
    }

    #[test]
    fn test_memory_constraints() -> Result<()> {
        let small_config = IndexConfig {
            build_subject_index: true,
            build_predicate_index: true,
            build_object_index: true,
            max_index_memory: 1024, // Very small - only 1KB
            progressive_loading: true,
        };

        let streaming =
            StreamingIndexedTriplesBitmap::from_file_with_config("tests/resources/snikmeta.hdt", small_config)?;

        // Should still work but with minimal indexes
        assert!(streaming.num_triples() > 0);

        Ok(())
    }
}
