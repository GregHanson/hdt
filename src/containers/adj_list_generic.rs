//! Generic adjacency list that works with both in-memory and file-based sequences

use crate::containers::{Bitmap, sequence_access::SequenceAccess};
use crate::triples::Id;
use std::cmp::Ordering;
use std::fmt::Debug;

/// Generic adjacency list with pluggable sequence implementation
///
/// This allows AdjList to use either:
/// - In-memory sequences (for TriplesBitmap)
/// - File-based sequences (for hybrid approach)
#[derive(Debug)]
pub struct AdjListGeneric<S: SequenceAccess> {
    /// Sequence implementation (in-memory or file-based)
    pub sequence: S,
    /// Helper bitmap for rank/select queries (always in memory - lightweight)
    pub bitmap: Bitmap,
}

impl<S: SequenceAccess> AdjListGeneric<S> {
    /// Create adjacency list with given sequence and bitmap
    pub fn new(sequence: S, bitmap: Bitmap) -> Self {
        Self { sequence, bitmap }
    }

    /// Combined size in bytes of the sequence and bitmap
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.bitmap.size_in_bytes()
    }

    /// Whether the given position represents the last child of the parent node
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.bitmap.at_last_sibling(word_index)
    }

    /// Get the ID at the given position
    pub fn get_id(&self, word_index: usize) -> Id {
        self.sequence.get(word_index) as Id
    }

    /// Number of entries
    pub fn len(&self) -> usize {
        self.sequence.len()
    }

    /// Whether the list is empty
    pub fn is_empty(&self) -> bool {
        self.sequence.is_empty()
    }

    /// Find the first position for the given ID, counting from 1
    pub fn find(&self, x: Id) -> usize {
        if x == 0 {
            return 0;
        }
        self.bitmap.select1(x - 1).unwrap() as usize + 1
    }

    /// Binary search in the sequence within given bounds
    fn bin_search(&self, element: usize, begin: usize, end: usize) -> Option<usize> {
        let mut low = begin;
        let mut high = end;
        while low < high {
            let mid = usize::midpoint(low, high);
            match self.sequence.get(mid).cmp(&element) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Find position of element y in the list x
    pub fn search(&self, x: usize, y: usize) -> Option<usize> {
        self.bin_search(y, self.find(x), self.last(x) + 1)
    }

    /// Find the last position for the given ID
    pub fn last(&self, x: Id) -> usize {
        self.find(x + 1) - 1
    }
}

// Type aliases for convenience
pub type AdjListInMemory = AdjListGeneric<crate::containers::sequence_access::InMemorySequence>;
pub type AdjListFileBased = AdjListGeneric<crate::containers::sequence_access::FileBasedSequence>;
