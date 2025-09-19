use crate::triples::triple_access::{
    AdvancedTripleAccess, QueryCost, QueryStrategy, Result, TimeComplexity, TripleAccess, TripleStatistics,
};
use crate::triples::{Error, Id, Order, TripleId, TriplesBitmap};
use std::cmp::Ordering;
use sucds::Serializable;

impl TripleAccess for TriplesBitmap {
    fn num_triples(&self) -> usize {
        self.adjlist_z.len()
    }

    fn order(&self) -> Order {
        self.order.clone()
    }

    fn size_in_bytes(&self) -> usize {
        self.adjlist_z.size_in_bytes() + self.op_index.size_in_bytes() + self.wavelet_y.size_in_bytes()
    }

    fn find_y(&self, subject_id: Id) -> Result<usize> {
        if subject_id == 0 {
            return Ok(0);
        }
        match self.bitmap_y.select1(subject_id - 1) {
            Some(pos) => Ok(pos + 1),
            None => Err(Error::External(
                format!("invalid s_id {subject_id}, there are only {} subjects", self.bitmap_y.num_ones()).into(),
            )),
        }
    }

    fn last_y(&self, subject_id: Id) -> Result<usize> {
        let next_pos = self.find_y(subject_id + 1);
        Ok(if next_pos > 0 { next_pos - 1 } else { 0 })
    }

    fn search_y(&self, subject_id: Id, predicate_id: Id) -> Result<Option<usize>> {
        let start = self.find_y(subject_id);
        let end = self.last_y(subject_id) + 1;
        Ok(self.bin_search_y(predicate_id, start, end))
    }

    fn get_object(&self, pos_z: usize) -> Result<Id> {
        Ok(self.adjlist_z.sequence.get(pos_z))
    }

    fn get_predicate(&self, pos_y: usize) -> Result<Id> {
        Ok(self.wavelet_y.access(pos_y).unwrap_or(0))
    }

    fn get_object_positions(&self, object_id: Id) -> Result<Vec<usize>> {
        // Use the existing OpIndex for efficient object queries
        if object_id == 0 || object_id > self.op_index.bitmap.num_ones() {
            return Ok(Vec::new());
        }

        let start = self.op_index.find(object_id);
        let end = self.op_index.last(object_id);

        let mut positions = Vec::new();
        for i in start..=end {
            if i < self.op_index.sequence.len() {
                // CompactVector's get method returns the value directly
                positions.push(self.op_index.sequence.get_int(i).unwrap());
            }
        }
        Ok(positions)
    }

    fn has_subject_index(&self) -> bool {
        true
    }
    fn has_predicate_index(&self) -> bool {
        true
    }
    fn has_object_index(&self) -> bool {
        true
    }

    fn get_triple_at_position(&self, position: usize) -> Result<TripleId> {
        if position >= self.num_triples() {
            return Err(Error::External("Position out of bounds".to_string().into()));
        }

        // Get object from Z position
        let object = self.get_object(position)?;

        // Get Y position from Z position using bitmap
        let pos_y = self.adjlist_z.bitmap.rank(position);
        let predicate = self.get_predicate(pos_y)?;

        // Find subject by reverse lookup in bitmap_y
        // This is expensive but correct
        let mut subject = 1;
        while subject <= self.bitmap_y.num_ones() {
            let start_y = self.find_y(subject);
            let end_y = if subject < self.bitmap_y.num_ones() {
                self.find_y(subject + 1) - 1
            } else {
                self.bitmap_y.len() - 1
            };

            if pos_y >= start_y && pos_y <= end_y {
                return self.coord_to_triple(subject, predicate, object);
            }
            subject += 1;
        }

        Err(Error::External("Could not find subject for position".to_string().into()))
    }
}

impl AdvancedTripleAccess for TriplesBitmap {
    fn estimate_query_cost(&self, pattern: TripleId) -> QueryCost {
        let [s, p, o] = pattern;

        match (s != 0, p != 0, o != 0) {
            // SPO - specific triple
            (true, true, true) => QueryCost {
                estimated_results: 1,
                estimated_time_complexity: TimeComplexity::Logarithmic,
                memory_required: 0,
                preferred_strategy: QueryStrategy::DirectIndex,
            },

            // SP? - subject-predicate pattern
            (true, true, false) => {
                let start = self.find_y(s);
                let end = self.last_y(s);
                let predicate_count = if let Some(pos) = self.search_y(s, p) {
                    // Count objects for this subject-predicate pair
                    let mut count = 0;
                    let start_z = self.adjlist_z.bitmap.select1(pos).unwrap_or(0) as usize;
                    let end_z = if pos + 1 < self.bitmap_y.len() {
                        self.adjlist_z.bitmap.select1(pos + 1).unwrap_or(self.adjlist_z.len()) as usize
                    } else {
                        self.adjlist_z.len()
                    };
                    end_z - start_z
                } else {
                    0
                };

                QueryCost {
                    estimated_results: predicate_count,
                    estimated_time_complexity: TimeComplexity::Logarithmic,
                    memory_required: 0,
                    preferred_strategy: QueryStrategy::DirectIndex,
                }
            }

            // S?? - subject pattern
            (true, false, false) => {
                let start = self.find_y(s);
                let end = self.last_y(s);

                QueryCost {
                    estimated_results: end - start + 1,
                    estimated_time_complexity: TimeComplexity::Logarithmic,
                    memory_required: 0,
                    preferred_strategy: QueryStrategy::SubjectScan,
                }
            }

            // ??O - object pattern
            (false, false, true) => {
                let positions = self.get_object_positions(o).unwrap_or_default();

                QueryCost {
                    estimated_results: positions.len(),
                    estimated_time_complexity: TimeComplexity::Logarithmic,
                    memory_required: 0,
                    preferred_strategy: QueryStrategy::ObjectScan,
                }
            }

            // ?P? - predicate pattern
            (false, true, false) => {
                // Count occurrences of predicate in wavelet matrix
                let count =
                    (0..self.wavelet_y.len()).filter(|&i| self.wavelet_y.access(i).unwrap_or(0) == p).count();

                QueryCost {
                    estimated_results: count,
                    estimated_time_complexity: TimeComplexity::Linear,
                    memory_required: 0,
                    preferred_strategy: QueryStrategy::PredicateScan,
                }
            }

            // ??? - all triples
            (false, false, false) => QueryCost {
                estimated_results: self.num_triples(),
                estimated_time_complexity: TimeComplexity::Linear,
                memory_required: 0,
                preferred_strategy: QueryStrategy::FullScan,
            },

            // Other patterns
            _ => QueryCost {
                estimated_results: self.num_triples() / 2, // Rough estimate
                estimated_time_complexity: TimeComplexity::Linear,
                memory_required: 0,
                preferred_strategy: QueryStrategy::FullScan,
            },
        }
    }

    fn get_statistics(&self) -> TripleStatistics {
        let num_subjects = self.bitmap_y.num_ones();
        let num_predicates = self.wavelet_y.iter().max().unwrap_or(0);
        let num_objects = self.adjlist_z.sequence.into_iter().max().unwrap_or(0);
        let num_triples = self.num_triples();

        // Calculate predicate frequencies
        let mut predicate_counts = std::collections::HashMap::new();
        for i in 0..self.wavelet_y.len() {
            let pred = self.wavelet_y.access(i).unwrap_or(0);
            *predicate_counts.entry(pred).or_insert(0) += 1;
        }

        let mut most_frequent_predicates: Vec<(Id, usize)> = predicate_counts.into_iter().collect();
        most_frequent_predicates.sort_by(|a, b| b.1.cmp(&a.1));
        most_frequent_predicates.truncate(10); // Top 10

        TripleStatistics {
            num_subjects,
            num_predicates,
            num_objects,
            num_triples,
            avg_predicates_per_subject: num_triples as f64 / num_subjects as f64,
            avg_objects_per_predicate: num_triples as f64 / num_predicates as f64,
            most_frequent_predicates,
            memory_usage: self.size_in_bytes(),
        }
    }

    fn optimize_for_patterns(&mut self, _patterns: &[TripleId]) -> Result<()> {
        // TriplesBitmap is already fully optimized with all indexes
        // No additional optimization needed
        Ok(())
    }
}

// Note: bin_search_y method already exists in the main TriplesBitmap implementation
