use crate::triples::{StreamingTriplesBitmap, StreamingIndexedTriplesBitmap, Error, Id, Order, TripleId};
use crate::triples::triple_access::{TripleAccess, Result, AdvancedTripleAccess, QueryCost, TimeComplexity, QueryStrategy, TripleStatistics};

// Implementation for basic StreamingTriplesBitmap
impl TripleAccess for StreamingTriplesBitmap {
    fn num_triples(&self) -> usize {
        self.num_triples()
    }

    fn order(&self) -> Order {
        self.order.clone()
    }

    fn size_in_bytes(&self) -> usize {
        self.size_in_bytes()
    }

    fn find_y(&self, subject_id: Id) -> Result<usize> {
        self.find_y(subject_id)
    }

    fn search_y(&self, subject_id: Id, predicate_id: Id) -> Result<Option<usize>> {
        // Basic streaming doesn't have efficient search, so we do a linear scan
        let start = self.find_y(subject_id)?;
        let end = self.last_y(subject_id)?;

        for pos in start..=end {
            if self.get_predicate(pos)? == predicate_id {
                return Ok(Some(pos));
            }
        }
        Ok(None)
    }

    fn get_object(&self, pos_z: usize) -> Result<Id> {
        self.get_object(pos_z)
    }

    fn get_predicate(&self, pos_y: usize) -> Result<Id> {
        self.get_predicate(pos_y)
    }

    fn has_subject_index(&self) -> bool { false } // No cached indexes
    fn has_predicate_index(&self) -> bool { false }
    fn has_object_index(&self) -> bool { false }

    fn get_triple_at_position(&self, position: usize) -> Result<TripleId> {
        if position >= self.num_triples() {
            return Err(Error::External("Position out of bounds".to_string().into()));
        }

        // This requires reconstructing the subject from the position
        // which is expensive for streaming implementation
        Err(Error::External("get_triple_at_position not efficiently supported by basic streaming".to_string().into()))
    }
}

// Implementation for StreamingIndexedTriplesBitmap
impl TripleAccess for StreamingIndexedTriplesBitmap {
    fn num_triples(&self) -> usize {
        self.num_triples()
    }

    fn order(&self) -> Order {
        self.order.clone()
    }

    fn size_in_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.index_memory_usage()
    }

    fn find_y(&self, subject_id: Id) -> Result<usize> {
        self.find_y(subject_id)
    }

    fn search_y(&self, subject_id: Id, predicate_id: Id) -> Result<Option<usize>> {
        // Use cached indexes if available, otherwise fall back to scan
        if self.indexes.predicate_wavelet.is_some() {
            // With wavelet matrix, we can do binary search
            let start = self.find_y(subject_id)?;
            let end = self.last_y(subject_id)?;

            // Binary search implementation
            let mut low = start;
            let mut high = end + 1;

            while low < high {
                let mid = low + (high - low) / 2;
                match self.get_predicate(mid)?.cmp(&predicate_id) {
                    std::cmp::Ordering::Less => low = mid + 1,
                    std::cmp::Ordering::Greater => high = mid,
                    std::cmp::Ordering::Equal => return Ok(Some(mid)),
                }
            }
            Ok(None)
        } else {
            // Fall back to linear scan
            let start = self.find_y(subject_id)?;
            let end = self.last_y(subject_id)?;

            for pos in start..=end {
                if self.get_predicate(pos)? == predicate_id {
                    return Ok(Some(pos));
                }
            }
            Ok(None)
        }
    }

    fn get_object(&self, pos_z: usize) -> Result<Id> {
        self.get_object(pos_z)
    }

    fn get_predicate(&self, pos_y: usize) -> Result<Id> {
        self.get_predicate(pos_y)
    }

    fn get_object_positions(&self, object_id: Id) -> Result<Vec<usize>> {
        self.get_object_positions(object_id)
    }

    fn get_predicate_frequency(&self, predicate_id: Id) -> Result<Option<usize>> {
        if let Some(ref frequencies) = self.indexes.predicate_frequencies {
            Ok(frequencies.get(&predicate_id).copied())
        } else {
            Ok(None)
        }
    }

    fn has_subject_index(&self) -> bool {
        self.indexes.subject_bitmap.is_some()
    }

    fn has_predicate_index(&self) -> bool {
        self.indexes.predicate_wavelet.is_some()
    }

    fn has_object_index(&self) -> bool {
        self.indexes.object_index.is_some()
    }

    fn get_triple_at_position(&self, position: usize) -> Result<TripleId> {
        if position >= self.num_triples() {
            return Err(Error::External("Position out of bounds".to_string().into()));
        }

        // For indexed streaming, we can implement this more efficiently
        // if we have the right indexes
        Err(Error::External("get_triple_at_position not yet implemented for indexed streaming".to_string().into()))
    }
}

// Advanced implementations for the indexed version
impl AdvancedTripleAccess for StreamingIndexedTriplesBitmap {
    fn estimate_query_cost(&self, pattern: TripleId) -> QueryCost {
        let [s, p, o] = pattern;

        match (s != 0, p != 0, o != 0) {
            // SPO - specific triple
            (true, true, true) => {
                let time_complexity = if self.has_subject_index() && self.has_predicate_index() {
                    TimeComplexity::Logarithmic
                } else {
                    TimeComplexity::Linear
                };

                QueryCost {
                    estimated_results: 1,
                    estimated_time_complexity: time_complexity,
                    memory_required: 0,
                    preferred_strategy: if self.has_subject_index() {
                        QueryStrategy::DirectIndex
                    } else {
                        QueryStrategy::FullScan
                    },
                }
            },

            // S?? - subject pattern
            (true, false, false) => {
                let time_complexity = if self.has_subject_index() {
                    TimeComplexity::Logarithmic
                } else {
                    TimeComplexity::Linear
                };

                QueryCost {
                    estimated_results: self.num_triples() / self.get_metadata().num_subjects, // Average
                    estimated_time_complexity: time_complexity,
                    memory_required: 0,
                    preferred_strategy: if self.has_subject_index() {
                        QueryStrategy::SubjectScan
                    } else {
                        QueryStrategy::FullScan
                    },
                }
            },

            // ??O - object pattern
            (false, false, true) => {
                let time_complexity = if self.has_object_index() {
                    TimeComplexity::Constant
                } else {
                    TimeComplexity::Linear
                };

                QueryCost {
                    estimated_results: self.num_triples() / self.get_metadata().num_objects, // Average
                    estimated_time_complexity: time_complexity,
                    memory_required: 0,
                    preferred_strategy: if self.has_object_index() {
                        QueryStrategy::ObjectScan
                    } else {
                        QueryStrategy::FullScan
                    },
                }
            },

            // ?P? - predicate pattern
            (false, true, false) => {
                let estimated_results = if let Ok(Some(freq)) = self.get_predicate_frequency(p) {
                    freq
                } else {
                    self.num_triples() / self.get_metadata().num_predicates // Average
                };

                let time_complexity = if self.has_predicate_index() {
                    TimeComplexity::LogLinear
                } else {
                    TimeComplexity::Linear
                };

                QueryCost {
                    estimated_results,
                    estimated_time_complexity: time_complexity,
                    memory_required: 0,
                    preferred_strategy: if self.has_predicate_index() {
                        QueryStrategy::PredicateScan
                    } else {
                        QueryStrategy::FullScan
                    },
                }
            },

            // ??? - all triples
            (false, false, false) => QueryCost {
                estimated_results: self.num_triples(),
                estimated_time_complexity: TimeComplexity::Linear,
                memory_required: 0,
                preferred_strategy: QueryStrategy::FullScan,
            },

            // Other patterns - conservative estimates
            _ => QueryCost {
                estimated_results: self.num_triples() / 10, // Conservative estimate
                estimated_time_complexity: TimeComplexity::Linear,
                memory_required: 0,
                preferred_strategy: QueryStrategy::FullScan,
            },
        }
    }

    fn get_statistics(&self) -> TripleStatistics {
        let metadata = self.get_metadata();

        // If we have predicate frequencies, use them
        let most_frequent_predicates = if let Some(ref freq_map) = self.indexes.predicate_frequencies {
            let mut frequencies: Vec<(Id, usize)> = freq_map.iter().map(|(&k, &v)| (k, v)).collect();
            frequencies.sort_by(|a, b| b.1.cmp(&a.1));
            frequencies.into_iter().take(10).collect()
        } else {
            Vec::new()
        };

        TripleStatistics {
            num_subjects: metadata.num_subjects,
            num_predicates: metadata.num_predicates,
            num_objects: metadata.num_objects,
            num_triples: metadata.num_triples,
            avg_predicates_per_subject: metadata.num_triples as f64 / metadata.num_subjects as f64,
            avg_objects_per_predicate: metadata.num_triples as f64 / metadata.num_predicates as f64,
            most_frequent_predicates,
            memory_usage: self.size_in_bytes(),
        }
    }

    fn optimize_for_patterns(&mut self, patterns: &[TripleId]) -> Result<()> {
        // Analyze patterns to decide which indexes to build/rebuild
        let mut needs_subject_index = false;
        let mut needs_predicate_index = false;
        let mut needs_object_index = false;

        for &[s, p, o] in patterns {
            if s != 0 { needs_subject_index = true; }
            if p != 0 { needs_predicate_index = true; }
            if o != 0 { needs_object_index = true; }
        }

        // Update configuration based on pattern analysis
        let mut new_config = self.get_config().clone();

        // Only enable indexes that aren't already built and are needed
        if needs_subject_index && !self.has_subject_index() {
            new_config.build_subject_index = true;
        }
        if needs_predicate_index && !self.has_predicate_index() {
            new_config.build_predicate_index = true;
        }
        if needs_object_index && !self.has_object_index() {
            new_config.build_object_index = true;
        }

        // Rebuild indexes if configuration changed
        if new_config.build_subject_index != self.get_config().build_subject_index ||
           new_config.build_predicate_index != self.get_config().build_predicate_index ||
           new_config.build_object_index != self.get_config().build_object_index {

            self.update_config(new_config)?;
        }

        Ok(())
    }
}