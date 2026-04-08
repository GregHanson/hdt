/// In-memory RDF representation.
pub mod rdf;

/// Variable length numbers.
pub mod vbyte;

// byte containers
mod adj_list;
pub mod adj_list_generic;
pub mod bitmap;
pub mod bitmap_access;
pub mod sequence;
pub mod sequence_access;

// control info section reader
pub mod control_info;

pub use adj_list::AdjList;
pub use adj_list_generic::{AdjListGeneric, AdjListInMemory, AdjListMmap};
pub use bitmap::Bitmap;
pub use bitmap_access::{BitmapAccess, InMemoryBitmap, MmapBitmap};
pub use control_info::{ControlInfo, ControlType};
pub use sequence::Sequence;
pub use sequence_access::{InMemorySequence, MmapSequence, SequenceAccess};
