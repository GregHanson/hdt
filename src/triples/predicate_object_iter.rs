use crate::containers::{BitmapAccess, CompactVectorAccess, SequenceAccess};
use crate::triples::{Id, TriplesBitmapGeneric};
use std::cmp::Ordering;

// see filterPredSubj in "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36

/// Iterator over all subject IDs with a given predicate and object ID, answering an (?S,P,O) query.
/// Generic over sequence access type S and bitmap access type B for TriplesBitmapGeneric.
pub struct PredicateObjectIter<
    'a,
    S: SequenceAccess = crate::containers::InMemorySequence,
    C: crate::containers::CompactVectorAccess = crate::containers::InMemoryCompactVector,
    B: BitmapAccess = crate::containers::InMemoryBitmap,
> {
    triples: &'a TriplesBitmapGeneric<S, C, B>,
    pos_index: usize,
    max_index: usize,
}

impl<'a, S: SequenceAccess, C: CompactVectorAccess, B: BitmapAccess> PredicateObjectIter<'a, S, C, B> {
    /// Create a new iterator over all triples with the given predicate and object ID.
    /// Panics if the predicate or object ID is 0.
    pub fn new(triples: &'a TriplesBitmapGeneric<S, C, B>, p: Id, o: Id) -> Self {
        assert_ne!(0, p, "predicate 0 does not exist, cant iterate");
        assert_ne!(0, o, "object 0 does not exist, cant iterate");
        let mut low = triples.op_index.find(o);
        let mut high = triples.op_index.last(o);
        let get_y = |pos_index| {
            let pos_y = triples.op_index.get(pos_index);
            triples.wavelet_y.access(pos_y).unwrap() as Id
        };
        // Binary search with a twist:
        // Each value may occur multiple times, so we search for the left and right borders.
        while low <= high {
            let mut mid = usize::midpoint(low, high);
            match get_y(mid).cmp(&p) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => {
                    let mut left_high = mid;
                    while low < left_high {
                        mid = usize::midpoint(low, left_high);
                        match get_y(mid).cmp(&p) {
                            Ordering::Less => low = mid + 1,
                            Ordering::Greater => {
                                high = mid;
                                left_high = mid;
                            }
                            Ordering::Equal => left_high = mid,
                        }
                    }
                    // right border
                    let mut right_low = low;
                    while right_low < high {
                        mid = (right_low + high).div_ceil(2);
                        match get_y(mid).cmp(&p) {
                            Ordering::Greater => high = mid - 1,
                            _ => right_low = mid,
                        }
                    }
                    return PredicateObjectIter { triples, pos_index: low, max_index: high };
                }
            }
            if (high == 0 && low == 0) || (high == low && high == mid) {
                break;
            }
        }
        // not found
        PredicateObjectIter { triples, pos_index: 999, max_index: 0 }
    }
}

impl<S: SequenceAccess, C: crate::containers::CompactVectorAccess, B: BitmapAccess> Iterator
    for PredicateObjectIter<'_, S, C, B>
{
    type Item = Id;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_index > self.max_index {
            return None;
        }
        let pos_y = self.triples.op_index.get(self.pos_index);
        //let y = self.triples.wavelet_y.get(pos_y as usize) as Id;
        //println!(" op p {y}");
        let s = self.triples.bitmap_y.rank(pos_y) as Id + 1;
        self.pos_index += 1;
        Some(s)
    }
}
