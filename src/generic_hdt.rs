use crate::containers::{ControlInfo, control_info};
use crate::four_sect_dict::{self, IdKind};
use crate::header::Header;
use crate::triples::{Id, TripleId, TripleAccess, AdvancedTripleAccess, TriplesBitmap, StreamingTriplesBitmap, StreamingIndexedTriplesBitmap, IndexConfig};
use crate::{FourSectDict, header};
use bytesize::ByteSize;
use log::{debug, error};
use std::sync::Arc;
use std::path::Path;
use std::io::BufRead;

pub type Result<T> = core::result::Result<T, Error>;

/// Generic HDT structure that can work with different triple access implementations.
/// This allows choosing between memory-optimized, performance-optimized, or balanced approaches.
#[derive(Debug)]
pub struct GenericHdt<T: TripleAccess> {
    header: Header,
    /// Dictionary for translating between IDs and strings
    pub dict: FourSectDict,
    /// Triple access implementation (can be TriplesBitmap, StreamingTriplesBitmap, etc.)
    pub triples: T,
}

type StringTriple = [String; 3];

/// Configuration for creating HDT instances with different triple access strategies
#[derive(Debug, Clone)]
pub enum TripleAccessStrategy {
    /// Full memory loading with all indexes (traditional approach)
    FullMemory,
    /// Minimal memory with file-based access
    Streaming,
    /// Configurable memory usage with selective indexing
    IndexedStreaming(IndexConfig),
}

impl Default for TripleAccessStrategy {
    fn default() -> Self {
        TripleAccessStrategy::FullMemory
    }
}

/// The error type for the generic HDT implementation
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to read HDT control info")]
    ControlInfo(#[from] control_info::Error),
    #[error("failed to read HDT header")]
    Header(#[from] header::Error),
    #[error("failed to read HDT four section dictionary")]
    FourSectDict(#[from] four_sect_dict::Error),
    #[error("failed to read HDT triples section")]
    Triples(#[from] crate::triples::Error),
    #[error("IO Error")]
    Io(#[from] std::io::Error),
    #[error("Unsupported triple access strategy for this operation")]
    UnsupportedStrategy,
}

/// The error type for the `translate_id` method.
#[derive(thiserror::Error, Debug)]
#[error("cannot translate triple ID {t:?} to string triple: {e}")]
pub struct TranslateError {
    #[source]
    e: four_sect_dict::ExtractError,
    t: TripleId,
}

// Type aliases for common configurations
pub type TraditionalHdt = GenericHdt<TriplesBitmap>;
pub type StreamingHdt = GenericHdt<StreamingTriplesBitmap>;
pub type IndexedStreamingHdt = GenericHdt<StreamingIndexedTriplesBitmap>;

impl<T: TripleAccess> GenericHdt<T> {
    /// Get the number of triples
    pub fn num_triples(&self) -> usize {
        self.triples.num_triples()
    }

    /// Get total memory usage
    pub fn size_in_bytes(&self) -> usize {
        self.dict.size_in_bytes() + self.triples.size_in_bytes()
    }

    /// Search for triples matching a pattern (0 = wildcard)
    pub fn search_triples(&self, pattern: TripleId) -> Result<Vec<TripleId>> {
        let [s, p, o] = pattern;
        let mut results = Vec::new();

        match (s != 0, p != 0, o != 0) {
            // SPO - specific triple
            (true, true, true) => {
                if let Ok(Some(pos)) = self.triples.search_y(s, p) {
                    // Find objects for this subject-predicate pair
                    // This is a simplified implementation
                    if let Ok(obj) = self.triples.get_object(pos) {
                        if obj == o {
                            results.push([s, p, o]);
                        }
                    }
                }
            },

            // SP? - subject-predicate pattern
            (true, true, false) => {
                if let Ok(Some(pos)) = self.triples.search_y(s, p) {
                    // Get all objects for this subject-predicate pair
                    // This needs more sophisticated implementation based on the storage format
                    if let Ok(obj) = self.triples.get_object(pos) {
                        results.push([s, p, obj]);
                    }
                }
            },

            // S?? - subject pattern
            (true, false, false) => {
                let start = self.triples.find_y(s)?;
                let end = self.triples.last_y(s)?;

                for pos_y in start..=end {
                    if let (Ok(pred), Ok(obj)) = (
                        self.triples.get_predicate(pos_y),
                        self.triples.get_object(pos_y) // This mapping is implementation-specific
                    ) {
                        results.push([s, pred, obj]);
                    }
                }
            },

            // ??O - object pattern
            (false, false, true) => {
                let positions = self.triples.get_object_positions(o)?;
                for pos in positions {
                    // Reconstruct subject and predicate from position
                    // This is complex and implementation-specific
                    if let Ok(obj_at_pos) = self.triples.get_object(pos) {
                        if obj_at_pos == o {
                            // We need to find subject and predicate for this position
                            // This requires reverse lookup which is expensive
                            // For now, we'll leave this as a TODO
                        }
                    }
                }
            },

            // ??? - all triples (use iterator)
            (false, false, false) => {
                // This would use the iterator implementation
                // For now, return empty to avoid performance issues
                return Ok(Vec::new());
            },

            _ => {
                // Other patterns require full scan or sophisticated indexing
                return Ok(Vec::new());
            }
        }

        Ok(results)
    }

    /// Translate numeric triple to string triple
    pub fn translate_triple(&self, triple: TripleId) -> std::result::Result<StringTriple, TranslateError> {

        let subject = self.dict.id_to_string(triple[0], IdKind::Subject).map_err(|e| TranslateError { e, t: triple })?;//extract_subject(s).map_err(|e| TranslateError { e, t: triple })?;
        let predicate = self.dict.id_to_string(triple[1], IdKind::Predicate).map_err(|e| TranslateError { e, t: triple })?;
        let object = self.dict.id_to_string(triple[2], IdKind::Object).map_err(|e| TranslateError { e, t: triple })?;

        Ok([subject, predicate, object])
    }

    /// Get statistics about the HDT
    pub fn get_statistics(&self) -> crate::triples::TripleStatistics
    where
        T: AdvancedTripleAccess,
    {
        self.triples.get_statistics()
    }

    /// Estimate query cost for a given pattern
    pub fn estimate_query_cost(&self, pattern: TripleId) -> crate::triples::QueryCost
    where
        T: AdvancedTripleAccess,
    {
        self.triples.estimate_query_cost(pattern)
    }

    /// Optimize the triple storage for given query patterns
    pub fn optimize_for_patterns(&mut self, patterns: &[TripleId]) -> Result<()>
    where
        T: AdvancedTripleAccess,
    {
        self.triples.optimize_for_patterns(patterns).map_err(Error::Triples)
    }
}

// Constructors for different strategies
impl GenericHdt<TriplesBitmap> {
    /// Create traditional HDT with full memory loading
    pub fn read_traditional<R: BufRead>(mut reader: R) -> Result<Self> {
        ControlInfo::read(&mut reader)?;
        let header = Header::read(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        let dict = unvalidated_dict.validate()?;

        let hdt = GenericHdt { header, dict, triples };
        debug!("Traditional HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");

        Ok(hdt)
    }
}

impl GenericHdt<StreamingTriplesBitmap> {
    /// Create streaming HDT with minimal memory usage
    pub fn read_streaming<P: AsRef<Path>>(path: P) -> Result<Self> {
        // We need to read the header and dictionary, but use streaming for triples
        let mut file = std::fs::File::open(&path)?;
        let mut reader = std::io::BufReader::new(&file);

        ControlInfo::read(&mut reader)?;
        let header = Header::read(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let dict = unvalidated_dict.validate()?;

        // Create streaming triples from file
        let triples = StreamingTriplesBitmap::from_file(path)?;

        let hdt = GenericHdt { header, dict, triples };
        debug!("Streaming HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");

        Ok(hdt)
    }
}

impl GenericHdt<StreamingIndexedTriplesBitmap> {
    /// Create indexed streaming HDT with configurable memory usage
    pub fn read_indexed_streaming<P: AsRef<Path>>(path: P, config: IndexConfig) -> Result<Self> {
        // Read header and dictionary normally
        let mut file = std::fs::File::open(&path)?;
        let mut reader = std::io::BufReader::new(&file);

        ControlInfo::read(&mut reader)?;
        let header = Header::read(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let dict = unvalidated_dict.validate()?;

        // Create indexed streaming triples
        let triples = StreamingIndexedTriplesBitmap::from_file_with_config(path, config)?;

        let hdt = GenericHdt { header, dict, triples };
        debug!("Indexed Streaming HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");

        Ok(hdt)
    }

    /// Create with default configuration
    pub fn read_indexed_streaming_default<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::read_indexed_streaming(path, IndexConfig::default())
    }
}

// Factory method for creating HDT with different strategies
pub fn create_hdt<P: AsRef<Path>>(path: P, strategy: TripleAccessStrategy) -> Result<Box<dyn HdtTrait>> {
    match strategy {
        TripleAccessStrategy::FullMemory => {
            let file = std::fs::File::open(&path)?;
            let reader = std::io::BufReader::new(file);
            let hdt = GenericHdt::<TriplesBitmap>::read_traditional(reader)?;
            Ok(Box::new(hdt))
        },
        TripleAccessStrategy::Streaming => {
            let hdt = GenericHdt::<StreamingTriplesBitmap>::read_streaming(path)?;
            Ok(Box::new(hdt))
        },
        TripleAccessStrategy::IndexedStreaming(config) => {
            let hdt = GenericHdt::<StreamingIndexedTriplesBitmap>::read_indexed_streaming(path, config)?;
            Ok(Box::new(hdt))
        },
    }
}

/// Trait object interface for working with HDT instances regardless of implementation
pub trait HdtTrait: std::fmt::Debug {
    fn num_triples(&self) -> usize;
    fn size_in_bytes(&self) -> usize;
    fn search_triples(&self, pattern: TripleId) -> Result<Vec<TripleId>>;
    fn translate_triple(&self, triple: TripleId) -> std::result::Result<StringTriple, TranslateError>;
}

impl<T: TripleAccess> HdtTrait for GenericHdt<T> {
    fn num_triples(&self) -> usize {
        self.num_triples()
    }

    fn size_in_bytes(&self) -> usize {
        self.size_in_bytes()
    }

    fn search_triples(&self, pattern: TripleId) -> Result<Vec<TripleId>> {
        self.search_triples(pattern)
    }

    fn translate_triple(&self, triple: TripleId) -> std::result::Result<StringTriple, TranslateError> {
        self.translate_triple(triple)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_different_strategies() -> Result<()> {
        let path = "tests/resources/snikmeta.hdt";

        // Test traditional approach
        let traditional = create_hdt(path, TripleAccessStrategy::FullMemory)?;
        assert!(traditional.num_triples() > 0);

        // Test streaming approach
        let streaming = create_hdt(path, TripleAccessStrategy::Streaming)?;
        assert_eq!(streaming.num_triples(), traditional.num_triples());
        assert!(streaming.size_in_bytes() < traditional.size_in_bytes());

        // Test indexed streaming approach
        let indexed = create_hdt(path, TripleAccessStrategy::IndexedStreaming(IndexConfig::default()))?;
        assert_eq!(indexed.num_triples(), traditional.num_triples());
        assert!(indexed.size_in_bytes() < traditional.size_in_bytes());

        Ok(())
    }

    #[test]
    fn test_type_specific_creation() -> Result<()> {
        // Test creating specific types directly
        let traditional = {
            let file = std::fs::File::open("tests/resources/snikmeta.hdt")?;
            let reader = std::io::BufReader::new(file);
            GenericHdt::<TriplesBitmap>::read_traditional(reader)?
        };

        let streaming = GenericHdt::<StreamingTriplesBitmap>::read_streaming("tests/resources/snikmeta.hdt")?;

        let indexed = GenericHdt::<StreamingIndexedTriplesBitmap>::read_indexed_streaming_default("tests/resources/snikmeta.hdt")?;

        // All should have same number of triples
        assert_eq!(traditional.num_triples(), streaming.num_triples());
        assert_eq!(traditional.num_triples(), indexed.num_triples());

        // Memory usage should differ
        assert!(streaming.size_in_bytes() < traditional.size_in_bytes());
        assert!(indexed.size_in_bytes() < traditional.size_in_bytes());

        Ok(())
    }
}