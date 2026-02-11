use crate::containers::{BitmapAccess, CompactVectorAccess, SequenceAccess};
use crate::triples::{Id, TripleId, TriplesBitmapGeneric};

/// Iterator over all triples with a given property ID, answering an (?S,P,?O) query.
/// Generic over sequence access type S and bitmap access type B for TriplesBitmapGeneric.
pub struct PredicateIter<
    'a,
    S: SequenceAccess = crate::containers::InMemorySequence,
    C: crate::containers::CompactVectorAccess = crate::containers::InMemoryCompactVector,
    B: BitmapAccess = crate::containers::InMemoryBitmap,
> {
    triples: &'a TriplesBitmapGeneric<S, C, B>,
    s: Id,
    p: Id,
    i: usize,
    os: usize,
    pos_z: usize,
    occs: usize,
}

impl<'a, S: SequenceAccess, C: CompactVectorAccess, B: BitmapAccess> PredicateIter<'a, S, C, B> {
    /// Create a new iterator over all triples with the given property ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmapGeneric<S, C, B>, p: Id) -> Self {
        assert!(p != 0, "object 0 does not exist, cant iterate");
        let occs = triples.wavelet_y.rank(triples.wavelet_y.len(), p as usize).unwrap();
        //println!("the predicate {} is used by {} subjects in the index", p, occs);
        PredicateIter { triples, p, i: 0, pos_z: 0, os: 0, s: 0, occs }
    }
}

impl<S: SequenceAccess, C: crate::containers::CompactVectorAccess, B: BitmapAccess> Iterator
    for PredicateIter<'_, S, C, B>
{
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.occs {
            return None;
        }
        if self.os == 0 {
            // Algorithm 1 findSubj from Martinez et al. 2012 ******
            let pos_y = self.triples.wavelet_y.select(self.i, self.p as usize).unwrap();
            self.s = self.triples.bitmap_y.rank(pos_y) as Id + 1;
            // *****************************************************
            // SP can have multiple O
            self.pos_z = self.triples.adjlist_z.find(pos_y as Id);
            let pos_z_end = self.triples.adjlist_z.last(pos_y as Id);
            //println!("**** found predicate {} between {} and {} (inclusive)", self.p, self.pos_z, pos_z_end);
            self.os = pos_z_end - self.pos_z;
        } else {
            self.os -= 1;
            self.pos_z += 1;
        }

        let o = self.triples.adjlist_z.get_id(self.pos_z);
        if self.os == 0 {
            self.i += 1;
        }
        Some(self.triples.coord_to_triple(self.s, self.p, o).unwrap())
    }
}
