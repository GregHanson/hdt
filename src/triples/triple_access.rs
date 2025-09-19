use crate::triples::{Error, Id, Order, TripleId};
use std::fmt::Debug;

pub type Result<T> = core::result::Result<T, Error>;

/// Trait for accessing RDF triples stored in HDT format.
///
/// This trait abstracts over different triple storage strategies:
/// - `TriplesBitmap`: Full in-memory storage with all indexes
/// - `StreamingTriplesBitmap`: Minimal memory, file-based access
/// - `StreamingIndexedTriplesBitmap`: Configurable memory/performance trade-off
///
/// The trait provides a common interface for querying triples regardless of
/// the underlying storage implementation.
pub trait TripleAccess: Debug + Send + Sync {
    /// Get the number of triples in the dataset
    fn num_triples(&self) -> usize;

    /// Get the triple ordering (SPO, SOP, etc.)
    fn order(&self) -> Order;

    /// Get memory usage in bytes of the triple storage
    fn size_in_bytes(&self) -> usize;

    /// Find the Y position (predicate layer) for a given subject ID
    /// Returns the starting position in the Y sequence for this subject
    fn find_y(&self, subject_id: Id) -> Result<usize>;

    /// Find the last Y position for a given subject ID
    fn last_y(&self, subject_id: Id) -> Result<usize> {
        if subject_id == 0 {
            return Ok(0);
        }
        // Default implementation: find_y of next subject - 1
        let next_pos = self.find_y(subject_id + 1)?;
        Ok(if next_pos > 0 { next_pos - 1 } else { 0 })
    }

    /// Search for a specific subject-predicate pair in the Y layer
    /// Returns the position if found, None otherwise
    fn search_y(&self, subject_id: Id, predicate_id: Id) -> Result<Option<usize>>;

    /// Get the object ID at a specific position in the Z sequence
    fn get_object(&self, pos_z: usize) -> Result<Id>;

    /// Get the predicate ID at a specific position in the Y sequence
    fn get_predicate(&self, pos_y: usize) -> Result<Id>;

    /// Convert layer coordinates to a triple ID based on the ordering
    fn coord_to_triple(&self, x: Id, y: Id, z: Id) -> Result<TripleId> {
        if x == 0 || y == 0 || z == 0 {
            return Err(Error::TripleComponentZero(x, y, z));
        }
        match self.order() {
            Order::SPO => Ok([x, y, z]),
            Order::SOP => Ok([x, z, y]),
            Order::PSO => Ok([y, x, z]),
            Order::POS => Ok([y, z, x]),
            Order::OSP => Ok([z, x, y]),
            Order::OPS => Ok([z, y, x]),
            Order::Unknown => Err(Error::UnknownTriplesOrder),
        }
    }

    /// Get all positions where a specific object appears (optional optimization)
    fn get_object_positions(&self, object_id: Id) -> Result<Vec<usize>> {
        // Default implementation: linear scan
        let mut positions = Vec::new();
        for pos in 0..self.num_triples() {
            if self.get_object(pos)? == object_id {
                positions.push(pos);
            }
        }
        Ok(positions)
    }

    /// Get frequency information for predicates (optional optimization)
    fn get_predicate_frequency(&self, predicate_id: Id) -> Result<Option<usize>> {
        // Default implementation: not available
        let _ = predicate_id;
        Ok(None)
    }

    /// Check if specific optimizations/indexes are available
    fn has_subject_index(&self) -> bool { true } // Most implementations support this
    fn has_predicate_index(&self) -> bool { false } // Override if available
    fn has_object_index(&self) -> bool { false } // Override if available

    /// Iterator support - return triple at specific global position
    fn get_triple_at_position(&self, position: usize) -> Result<TripleId> {
        // This requires mapping from global position to x,y,z coordinates
        // Implementation depends on the specific storage format
        // Default implementation assumes SPO ordering and simple mapping
        if position >= self.num_triples() {
            return Err(Error::External("Position out of bounds".to_string().into()));
        }

        // For a proper implementation, this would need to:
        // 1. Find which subject this position belongs to
        // 2. Find which predicate within that subject
        // 3. Get the object at the Z position
        // This is complex and implementation-specific

        // Simplified version - implementors should override this
        Err(Error::External("get_triple_at_position not implemented for this storage type".to_string().into()))
    }
}

/// Iterator wrapper that works with any TripleAccess implementation
pub struct TripleAccessIterator<'a> {
    triple_access: &'a dyn TripleAccess,
    current_position: usize,
}

impl<'a> TripleAccessIterator<'a> {
    pub fn new(triple_access: &'a dyn TripleAccess) -> Self {
        Self {
            triple_access,
            current_position: 0,
        }
    }
}

impl<'a> Iterator for TripleAccessIterator<'a> {
    type Item = TripleId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position >= self.triple_access.num_triples() {
            return None;
        }

        match self.triple_access.get_triple_at_position(self.current_position) {
            Ok(triple) => {
                self.current_position += 1;
                Some(triple)
            }
            Err(_) => None, // Could log error here
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.triple_access.num_triples().saturating_sub(self.current_position);
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for TripleAccessIterator<'a> {}

/// Subject-specific iterator trait for more advanced iteration patterns
pub trait SubjectIterator {
    /// Create iterator for all triples with a specific subject
    fn with_subject(&self, subject_id: Id) -> Result<Box<dyn Iterator<Item = TripleId> + '_>>;

    /// Create iterator for specific subject-predicate pattern
    fn with_subject_predicate(&self, subject_id: Id, predicate_id: Id) -> Result<Box<dyn Iterator<Item = TripleId> + '_>>;

    /// Create iterator for specific triple pattern (0 = wildcard)
    fn with_pattern(&self, pattern: TripleId) -> Result<Box<dyn Iterator<Item = TripleId> + '_>>;
}

/// Additional trait for implementations that support advanced querying
pub trait AdvancedTripleAccess: TripleAccess {
    /// Estimate cost of different query strategies
    fn estimate_query_cost(&self, pattern: TripleId) -> QueryCost;

    /// Get statistics about the triple store
    fn get_statistics(&self) -> TripleStatistics;

    /// Optimize internal structures based on query patterns
    fn optimize_for_patterns(&mut self, patterns: &[TripleId]) -> Result<()>;
}

/// Query cost estimation for optimization
#[derive(Debug, Clone)]
pub struct QueryCost {
    pub estimated_results: usize,
    pub estimated_time_complexity: TimeComplexity,
    pub memory_required: usize,
    pub preferred_strategy: QueryStrategy,
}

#[derive(Debug, Clone)]
pub enum TimeComplexity {
    Constant,           // O(1)
    Logarithmic,        // O(log n)
    Linear,             // O(n)
    LogLinear,          // O(n log n)
    Quadratic,          // O(n²)
}

#[derive(Debug, Clone)]
pub enum QueryStrategy {
    DirectIndex,        // Use a direct index lookup
    SubjectScan,        // Scan from subject
    PredicateScan,      // Scan from predicate
    ObjectScan,         // Scan from object
    FullScan,           // Full linear scan
}

/// Statistics about the triple store
#[derive(Debug, Clone)]
pub struct TripleStatistics {
    pub num_subjects: usize,
    pub num_predicates: usize,
    pub num_objects: usize,
    pub num_triples: usize,
    pub avg_predicates_per_subject: f64,
    pub avg_objects_per_predicate: f64,
    pub most_frequent_predicates: Vec<(Id, usize)>,
    pub memory_usage: usize,
}