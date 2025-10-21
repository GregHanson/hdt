/// In-memory RDF representation.
pub mod rdf;

/// Variable length numbers.
pub mod vbyte;

// byte containers
mod adj_list;
pub mod bitmap;
pub mod sequence;
pub mod sequence_access;
pub mod adj_list_generic;
pub mod compact_vector_access;

// control info section reader
pub mod control_info;

pub use adj_list::AdjList;
pub use bitmap::Bitmap;
pub use control_info::{ControlInfo, ControlType};
pub use sequence::Sequence;
pub use sequence_access::{SequenceAccess, InMemorySequence, FileBasedSequence};
pub use adj_list_generic::{AdjListGeneric, AdjListInMemory, AdjListFileBased};
pub use compact_vector_access::{CompactVectorAccess, InMemoryCompactVector, FileBasedCompactVector};
