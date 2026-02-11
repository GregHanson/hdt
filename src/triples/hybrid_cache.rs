//! Hybrid cache for TriplesBitmap that stores metadata for creating file-based readers

use std::path::PathBuf;

/// Metadata for creating hybrid/file-based TriplesBitmap readers
#[derive(Debug, Clone)]
pub struct CacheMetadata {
    /// Path to the HDT file
    pub hdt_path: PathBuf,
    /// Offset to bitmap_y in the file
    pub bitmap_y_offset: u64,
    /// Offset to bitmap_z in the file
    pub bitmap_z_offset: u64,
    /// Offset to sequence_y in the file
    pub sequence_y_offset: u64,
    /// Offset to sequence_z in the file
    pub sequence_z_offset: u64,
    /// Offset to op_index sequence in the file
    pub op_index_sequence_offset: u64,
    /// Offset to op_index bitmap in the file
    pub op_index_bitmap_offset: u64,
}

impl CacheMetadata {
    /// Create new cache metadata
    pub fn new(
        hdt_path: PathBuf,
        bitmap_y_offset: u64,
        bitmap_z_offset: u64,
        sequence_y_offset: u64,
        sequence_z_offset: u64,
        op_index_sequence_offset: u64,
        op_index_bitmap_offset: u64,
    ) -> Self {
        Self {
            hdt_path,
            bitmap_y_offset,
            bitmap_z_offset,
            sequence_y_offset,
            sequence_z_offset,
            op_index_sequence_offset,
            op_index_bitmap_offset,
        }
    }
}

/// Hybrid cache that can create both in-memory and file-based TriplesBitmap readers
pub struct HybridCache {
    metadata: CacheMetadata,
}

impl HybridCache {
    /// Create a new hybrid cache from metadata
    pub fn new(metadata: CacheMetadata) -> Self {
        Self { metadata }
    }

    /// Get the cache metadata
    pub fn metadata(&self) -> &CacheMetadata {
        &self.metadata
    }
}
