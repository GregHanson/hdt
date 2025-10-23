//! Generic adjacency list that works with both in-memory and file-based sequences and bitmaps

use crate::containers::{bitmap_access::BitmapAccess, sequence_access::SequenceAccess};
use crate::triples::Id;
use std::cmp::Ordering;
use std::fmt::Debug;

/// Generic adjacency list with pluggable sequence and bitmap implementations
///
/// This allows AdjList to use either:
/// - In-memory sequences and bitmaps (for TriplesBitmap)
/// - File-based sequences and bitmaps (for hybrid approach with minimal memory)
#[derive(Debug)]
pub struct AdjListGeneric<S: SequenceAccess, B: BitmapAccess> {
    /// Sequence implementation (in-memory or file-based)
    pub sequence: S,
    /// Bitmap for rank/select queries (in-memory or file-based)
    pub bitmap: B,
}

impl<S: SequenceAccess, B: BitmapAccess> AdjListGeneric<S, B> {
    /// Create adjacency list with given sequence and bitmap
    pub fn new(sequence: S, bitmap: B) -> Self {
        Self { sequence, bitmap }
    }

    /// Combined size in bytes of the sequence and bitmap
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.bitmap.size_in_bytes()
    }

    /// Whether the given position represents the last child of the parent node
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.bitmap.access(word_index)
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
pub type AdjListInMemory = AdjListGeneric<
    crate::containers::sequence_access::InMemorySequence,
    crate::containers::bitmap_access::InMemoryBitmap
>;
pub type AdjListFileBased = AdjListGeneric<
    crate::containers::sequence_access::FileBasedSequence,
    crate::containers::bitmap_access::FileBasedBitmap
>;
