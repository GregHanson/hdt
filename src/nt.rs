// //! *This module is available only if HDT is built with the experimental `"nt"` feature.*
use super::concurrent_interner::{Interner, Terms};
use crate::containers::rdf::Id;
use crate::header::Header;
use crate::triples::{Id as HdtId, TripleId, TriplesBitmap};
use crate::{DictSectPFC, FourSectDict, Hdt};
use bitset_core::BitSet;
use bytesize::ByteSize;
use log::{debug, error};
use oxrdf::{TermRef, Triple, TripleRef, vocab::xsd};
use oxttl::NTriplesParser;
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::fmt::{Display, Write as _};
use std::io::{Error, ErrorKind::InvalidData, Read};
use std::path::Path;
use std::thread;

pub type Result<T> = std::io::Result<T>;
type Simd = [u64; 4];
type Indices = Vec<Simd>;

/// Bytes read per block by [`HdtBuilder::load_nt_reader`]: enough that every
/// parser thread gets a few MiB of each block.
const NT_READER_BLOCK: usize = 64 << 20;

impl Hdt {
    /// Converts RDF N-Triples to HDT with a FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order.
    /// Literal escapes are decoded, so the dictionary holds term values, not N-Triples syntax.
    /// All triples including nul chars (U+0000), whether written `\u0000`, `\U00000000` or as a raw byte, are dropped as the dictionary cannot store them.
    /// To build one HDT from several inputs, use [`HdtBuilder`].
    /// *This function is available only if HDT is built with the experimental `"nt"` feature.*
    /// # Example
    /// ```
    /// let hdt = hdt::Hdt::read_nt("tests/resources/empty.nt").unwrap();
    /// ```
    pub fn read_nt(f: impl AsRef<Path>) -> Result<Self> {
        let f = f.as_ref();
        let base = format!("file://{}", f.canonicalize()?.display());
        let mut builder = HdtBuilder::new();
        builder.load_nt_file(f)?;
        builder.finish(&base)
    }

    /// Builds an HDT with a FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order
    /// from triples in memory, e.g. to write an existing RDF graph as HDT without going through a file.
    /// Terms are given in the HDT dictionary string format: IRIs without enclosing angle brackets,
    /// literals including quotes, e.g. `"example"@en` or `"123"^^<http://www.w3.org/2001/XMLSchema#integer>`,
    /// and blank nodes as `_:b1`. This is the same format that [`Hdt::triples_all`] returns.
    /// The base IRI denotes the dataset in the header.
    /// All triples including nul chars (U+0000), whether written `\u0000`, `\U00000000` or as a raw byte, are dropped as the dictionary cannot store them.
    /// *This function is available only if HDT is built with the experimental `"nt"` feature.*
    /// # Example
    /// ```
    /// let triples = [["http://ex.org/s", "http://ex.org/p", "http://ex.org/o"]];
    /// let hdt = hdt::Hdt::from_triples(triples, "http://example.org/mydataset").unwrap();
    /// assert_eq!(hdt.triples_with_pattern(None, None, Some("http://ex.org/o")).count(), 1);
    /// ```
    pub fn from_triples<S: AsRef<str>>(triples: impl IntoIterator<Item = [S; 3]>, base_iri: &str) -> Result<Self> {
        let mut builder = HdtBuilder::new();
        builder.load_str_triples(triples)?;
        builder.finish(base_iri)
    }

    fn from_parsed_terms(pool: ParsedTerms, base: &Id, original_size: Option<u64>) -> Result<Self> {
        const BLOCK_SIZE: usize = 16;

        let (dict, mut encoded_triples) = dict_triples(pool, BLOCK_SIZE)?;
        let num_triples = encoded_triples.len();
        // Sort by final HDT ID (SPO order) before feeding into TriplesBitmap.
        encoded_triples.par_sort_unstable();
        // Move (don't borrow) the encoded triples in so TriplesBitmap can free
        // them after its build loop, before its op-index peak.
        let triples = TriplesBitmap::from_triples(encoded_triples);

        let header = Header { format: "ntriples".to_owned(), length: 0, body: BTreeSet::new() };
        let mut hdt = Hdt { header, dict, triples, predicate_counts: std::sync::OnceLock::new() };
        hdt.fill_header(base, BLOCK_SIZE, num_triples, original_size);
        // This crate's `Header::write` does not auto-recompute the serialized
        // header length, so set it explicitly here or the HDT fails to
        // round-trip through `Hdt::read`.
        hdt.recompute_header_length()?;

        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    /// Populate HDT header fields.
    /// Some fields may be optional, populating same triples as those in C++ version for now.
    fn fill_header(&mut self, base: &Id, block_size: usize, num_triples: usize, original_size: Option<u64>) {
        use crate::containers::rdf::Term::Literal as Lit;
        use crate::containers::rdf::{Literal, Term, Triple};
        use crate::vocab::*;

        const ORDER: &str = "SPO";

        macro_rules! literal {
            ($s:expr, $p:expr, $o:expr) => {
                self.header.body.insert(Triple::new($s.clone(), $p.to_owned(), Lit(Literal::new($o.to_string()))));
            };
        }
        macro_rules! insert_id {
            ($s:expr, $p:expr, $o:expr) => {
                self.header.body.insert(Triple::new($s.clone(), $p.to_owned(), Term::Id($o.clone())));
            };
        }
        literal!(base, RDF_TYPE, HDT_CONTAINER);
        literal!(base, RDF_TYPE, VOID_DATASET);
        literal!(base, VOID_TRIPLES, num_triples);
        literal!(base, VOID_PROPERTIES, self.dict.predicates.num_strings);
        let [d_s, d_o] =
            [&self.dict.subjects, &self.dict.objects].map(|s| s.num_strings + self.dict.shared.num_strings);
        literal!(base, VOID_DISTINCT_SUBJECTS, d_s);
        literal!(base, VOID_DISTINCT_OBJECTS, d_o);
        // // TODO: Add more VOID Properties. E.g. void:classes

        // // Structure
        let stats_id = Id::Blank("statistics".to_owned());
        let pub_id = Id::Blank("publicationInformation".to_owned());
        let format_id = Id::Blank("format".to_owned());
        let dict_id = Id::Blank("dictionary".to_owned());
        let triples_id = Id::Blank("triples".to_owned());
        insert_id!(base, HDT_STATISTICAL_INFORMATION, stats_id);
        insert_id!(base, HDT_STATISTICAL_INFORMATION, pub_id);
        insert_id!(base, HDT_FORMAT_INFORMATION, format_id);
        insert_id!(format_id, HDT_DICTIONARY, dict_id);
        insert_id!(format_id, HDT_TRIPLES, triples_id);
        // DICTIONARY
        literal!(dict_id, HDT_DICT_SHARED_SO, self.dict.shared.num_strings);
        literal!(dict_id, HDT_DICT_MAPPING, "1");
        literal!(dict_id, HDT_DICT_SIZE_STRINGS, ByteSize(self.dict.size_in_bytes() as u64));
        literal!(dict_id, HDT_DICT_BLOCK_SIZE, block_size);
        // TRIPLES
        literal!(triples_id, DC_TERMS_FORMAT, HDT_TYPE_BITMAP);
        literal!(triples_id, HDT_NUM_TRIPLES, num_triples);
        literal!(triples_id, HDT_TRIPLES_ORDER, ORDER);
        // // Sizes
        if let Some(size) = original_size {
            literal!(stats_id, HDT_ORIGINAL_SIZE, size);
        }
        // a few bytes off because that literal itself is not counted
        literal!(stats_id, HDT_SIZE, ByteSize(self.size_in_bytes() as u64));
        // exclude for now to skip dependency on chrono
        //let datetime_str = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%z").to_string();
        //literal!(pub_id,DC_TERMS_ISSUED,datetime_str);
    }
}

/// Builds an HDT from RDF loaded in any number of calls (N-Triples files, N-Triples
/// streams, parsed triples, or a mix), then assembles it once in [`finish`](Self::finish).
///
/// Terms are stored as [`Hdt::read_nt`] stores them: literal escapes decoded, and
/// triples containing NUL characters dropped. Each load interns its terms and keeps
/// its triples as three `u32` term indices, so memory grows with the loaded data
/// just as with `read_nt`; duplicate triples are removed in `finish`. A load either
/// keeps all of its triples or, on error, none of them, and the builder stays usable.
/// *This type is available only if HDT is built with the experimental `"nt"` feature.*
/// # Example
/// ```
/// use hdt::oxrdf::{NamedNode, Triple};
///
/// let mut builder = hdt::HdtBuilder::new();
/// builder.load_nt_file("tests/resources/empty.nt")?;
/// let ex = |name: &str| NamedNode::new_unchecked(format!("http://example.org/{name}"));
/// builder.load_triples(&[Triple::new(ex("s"), ex("p"), ex("o"))])?;
/// let hdt = builder.finish("http://example.org/dataset")?;
/// assert_eq!(hdt.triples_all().count(), 1);
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct HdtBuilder {
    interner: Interner,
    /// Term indices into `interner`, in load order, duplicates included.
    triples: Vec<[u32; 3]>,
    /// Input bytes for the header's original size; `None` until something counts.
    original_size: Option<u64>,
}

impl Default for HdtBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HdtBuilder {
    pub fn new() -> Self {
        Self { interner: Interner::new(), triples: Vec::new(), original_size: None }
    }

    /// Load an N-Triples file, parsed in parallel. Its size counts toward the
    /// header's original size.
    pub fn load_nt_file(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let (triples, size) = self.parse_nt_file(path).map_err(|e| with_path(&e, path))?;
        self.commit(triples, size);
        Ok(())
    }

    fn parse_nt_file(&self, path: &Path) -> Result<(Vec<[u32; 3]>, u64)> {
        let size = std::fs::metadata(path)?.len();
        let readers = NTriplesParser::new().split_file_for_parallel_parsing(path, num_parsers())?;
        let interner = &self.interner;
        let triples = readers
            .into_par_iter()
            .flat_map_iter(|reader| {
                let mut bufs = TermBufs::default();
                reader.filter_map(move |triple| match triple {
                    Ok(triple) => intern_triple(interner, triple.as_ref(), &mut bufs).map(Ok),
                    Err(e) => Some(Err(parse_error(e))),
                })
            })
            .collect::<Result<Vec<[u32; 3]>>>()?;
        Ok((triples, size))
    }

    /// Load N-Triples from a stream such as a decompressor or a pipe. The stream is
    /// read in blocks of whole lines and each block is parsed in parallel, so the
    /// buffered input stays bounded however long the stream is. The bytes read
    /// count toward the header's original size.
    pub fn load_nt_reader(&mut self, reader: impl Read) -> Result<()> {
        self.load_nt_blocks(reader, NT_READER_BLOCK)
    }

    fn load_nt_blocks(&mut self, mut reader: impl Read, block_size: usize) -> Result<()> {
        let parsers = num_parsers();
        let interner = &self.interner;
        let mut triples: Vec<[u32; 3]> = Vec::new();
        let mut buf: Vec<u8> = Vec::with_capacity(block_size);
        let mut bytes = 0;
        loop {
            // Read a whole block on top of the carried-over partial line, so a
            // line longer than a block still makes progress.
            let read = reader.by_ref().take(block_size as u64).read_to_end(&mut buf)?;
            bytes += read as u64;
            let eof = read < block_size;
            // Parse whole lines only and carry the partial last line into the
            // next block. N-Triples statements never span lines.
            let cut = if eof { buf.len() } else { buf.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1) };
            if cut > 0 {
                let mut block = NTriplesParser::new()
                    .split_slice_for_parallel_parsing(&buf[..cut], parsers)
                    .into_par_iter()
                    .flat_map_iter(|parser| {
                        let mut bufs = TermBufs::default();
                        parser.filter_map(move |triple| match triple {
                            Ok(triple) => intern_triple(interner, triple.as_ref(), &mut bufs).map(Ok),
                            Err(e) => Some(Err(parse_error(e))),
                        })
                    })
                    .collect::<Result<Vec<[u32; 3]>>>()?;
                triples.append(&mut block);
                buf.drain(..cut);
            }
            if eof {
                break;
            }
        }
        self.commit(triples, bytes);
        Ok(())
    }

    /// Load already-parsed triples, e.g. from a Turtle or RDF/XML parser, interning
    /// them in parallel. Use [`add_original_size`](Self::add_original_size) to
    /// record the size of their source.
    pub fn load_triples(&mut self, triples: &[Triple]) -> Result<()> {
        let interner = &self.interner;
        self.triples.par_extend(
            triples
                .par_iter()
                .map_init(TermBufs::default, |bufs, triple| intern_triple(interner, triple.as_ref(), bufs))
                .flatten(),
        );
        Ok(())
    }

    /// Load triples whose terms are already in the HDT dictionary format described
    /// in [`Hdt::from_triples`], such as those returned by [`Hdt::triples_all`].
    pub fn load_str_triples<S: AsRef<str>>(&mut self, triples: impl IntoIterator<Item = [S; 3]>) -> Result<()> {
        let interner = &self.interner;
        self.triples.extend(
            triples
                .into_iter()
                .filter(|[s, p, o]| is_valid_triple(s.as_ref(), p.as_ref(), o.as_ref()))
                .map(|t| t.map(|term| interner.get_or_intern(term.as_ref()))),
        );
        Ok(())
    }

    /// Add `bytes` to the original input size recorded in the HDT header.
    /// [`load_nt_file`](Self::load_nt_file) and [`load_nt_reader`](Self::load_nt_reader)
    /// count their input themselves.
    pub fn add_original_size(&mut self, bytes: u64) {
        self.original_size = Some(self.original_size.unwrap_or(0) + bytes);
    }

    /// Number of triples loaded so far, counting duplicates.
    pub const fn len(&self) -> usize {
        self.triples.len()
    }

    /// Build the HDT: dictionary, triples and header, with `base_iri` naming the
    /// dataset in the header. Duplicate triples are removed here, and this is where
    /// memory use peaks.
    pub fn finish(self, base_iri: &str) -> Result<Hdt> {
        let Self { interner, triples, original_size } = self;
        Hdt::from_parsed_terms(ParsedTerms::new(interner, triples), &Id::Named(base_iri.to_owned()), original_size)
    }

    /// Append the triples of a successful load and count its input bytes.
    fn commit(&mut self, mut triples: Vec<[u32; 3]>, bytes: u64) {
        if self.triples.is_empty() {
            self.triples = triples;
        } else {
            self.triples.append(&mut triples);
        }
        self.add_original_size(bytes);
    }
}

/// N-Triples parser threads: two when the available parallelism cannot be
/// determined, as a single thread is around 38% slower; 16 as a sane upper limit.
fn num_parsers() -> usize {
    std::cmp::min(16, thread::available_parallelism().map_or(2, std::num::NonZero::get))
}

/// Per-thread buffers for rendering a triple's subject and object, reused so
/// that rendering a term does not allocate.
type TermBufs = [String; 2];

/// Intern `triple` in HDT dictionary form, or drop it (`None`) if a term holds
/// a NUL character, which the dictionary cannot store.
fn intern_triple(interner: &Interner, triple: TripleRef<'_>, bufs: &mut TermBufs) -> Option<[u32; 3]> {
    let [s, o] = bufs;
    write_dict_term(s, triple.subject.into());
    write_dict_term(o, triple.object);
    let p = triple.predicate.as_str();
    is_valid_triple(s, p, o)
        .then(|| [interner.get_or_intern(s), interner.get_or_intern(p), interner.get_or_intern(o)])
}

fn parse_error(error: impl Display) -> Error {
    Error::new(InvalidData, format!("Error reading N-Triples: {error}"))
}

/// Prefix an error from loading `path` with the path.
fn with_path(error: &Error, path: &Path) -> Error {
    Error::new(error.kind(), format!("{}: {error}", path.display()))
}

/// Interned input handed from [`HdtBuilder::finish`] to [`Hdt::from_parsed_terms`].
/// All term strings live inside the `Interner`; the triples hold `u32` term
/// indices (4 bytes each) instead of full strings, and the three bitsets track
/// which indices appear as subject / predicate / object.
struct ParsedTerms {
    triples: Vec<[u32; 3]>,
    interner: Interner,
    subjects: Indices,
    predicates: Indices,
    objects: Indices,
}

impl ParsedTerms {
    /// Derive the role bitsets (subject / predicate / object) from the interned
    /// triples. Indices are 0-based and dense, sized by the interner's term count.
    fn new(interner: Interner, triples: Vec<[u32; 3]>) -> Self {
        let block = [0u64; 4];
        let blocks = interner.len().div_ceil(256);
        let mut subjects: Indices = vec![block; blocks];
        let mut objects: Indices = vec![block; blocks];
        let mut predicates: Indices = vec![block; blocks];

        for [s, p, o] in &triples {
            subjects.bit_set(*s as usize);
            predicates.bit_set(*p as usize);
            objects.bit_set(*o as usize);
        }

        ParsedTerms { triples, interner, subjects, predicates, objects }
    }
}

/// ID map: indexed by term index (`u32` as `usize`), holds the final HDT id for
/// a term in a given role (subject/predicate/object), or 0 if it has no id in
/// that role. u32 fits: HDT ids are at most `num_strings` ≤ u32::MAX.
type IdMap = Vec<u32>;

fn is_valid_triple(s: &str, p: &str, o: &str) -> bool {
    if s.contains('\0') || p.contains('\0') || o.contains('\0') {
        log::warn!("invalid triple ({s},{p},{o}) with unsupported NUL characters");
        return false;
    }
    true
}

/// Convert a parsed/interned term pool to a dictionary and encoded triple IDs.
fn dict_triples(pool: ParsedTerms, block_size: usize) -> Result<(FourSectDict, Vec<TripleId>)> {
    let ParsedTerms { triples, interner, subjects, predicates, objects } = pool;

    // In parallel with dictionary build: sort + dedup triples (by term index
    // — this removes exact duplicate triples; the final SPO-ID sort happens
    // later, once we've assigned HDT ids).
    let sorter = thread::Builder::new().name("sorter".to_owned()).spawn(move || {
        let mut t = triples;
        t.par_sort_unstable();
        t.dedup();
        t
    })?;

    // Assign HDT ids in sorted-string order and build the compressed dict.
    // Returns three `index -> u32 id` lookup tables — direct array indexing
    // during encoding, no more binary-search-through-PFC.
    let (dict, subj_map, pred_map, obj_map) = {
        // Consume the interner into an arena-backed, index-addressable view (no
        // per-term copy), then drop it at the end of this block so the term
        // bytes are freed before the encoding peak.
        let terms = interner.into_terms();
        build_dict_and_id_maps(&terms, &subjects, &predicates, &objects, block_size)
    };
    // Bitsets served their purpose; drop before the encoding peak.
    drop(subjects);
    drop(predicates);
    drop(objects);

    // Drain the sorted index triples directly into HDT-id triples via the ID
    // maps. `into_par_iter` consumes the Vec so the index triples are freed
    // before this function returns — only `Vec<TripleId>` survives into
    // `TriplesBitmap::from_triples`.
    let sorted_triples = sorter.join().expect("NT sorter thread panicked");
    let encoded_triples: Vec<TripleId> = sorted_triples
        .into_par_iter()
        .map(|[s_idx, p_idx, o_idx]| {
            let s = subj_map[s_idx as usize] as HdtId;
            let p = pred_map[p_idx as usize] as HdtId;
            let o = obj_map[o_idx as usize] as HdtId;
            if s == 0 || p == 0 || o == 0 {
                error!("encoded triple [{s}, {p}, {o}] contains 0; term missing from dictionary");
            }
            [s, p, o]
        })
        .collect();

    drop(subj_map);
    drop(pred_map);
    drop(obj_map);

    Ok((dict, encoded_triples))
}

/// Render a parsed term into `buf` in HDT dictionary form: an IRI unbracketed,
/// a blank node as `_:id`, a literal quoted with its language tag or datatype.
///
/// A literal's body is its *decoded* value. N-Triples escapes are syntax,
/// not part of the value: `"tab\there"` in a file denotes a string holding
/// one tab, and that tab is what belongs in the dictionary. Rendering the
/// term with `to_string()` instead put the escape back in, so serializing
/// the HDT escaped it a second time and a tab came back out as a backslash
/// (#131).
///
/// The HDT format itself never pins this down, but decoded values are the
/// de facto convention: hdt-cpp (through serd) and hdt-java (through Jena)
/// both store them and escape only on serialization, as do
/// [`Hdt::from_triples`] and the Sophia adapter in this crate.
fn write_dict_term(buf: &mut String, term: TermRef<'_>) {
    buf.clear();
    match term {
        TermRef::NamedNode(n) => buf.push_str(n.as_str()),
        TermRef::BlankNode(b) => {
            buf.push_str("_:");
            buf.push_str(b.as_str());
        }
        TermRef::Literal(l) => {
            buf.push('"');
            buf.push_str(l.value());
            buf.push('"');
            match l.language() {
                Some(language) => {
                    buf.push('@');
                    buf.push_str(language);
                }
                // A simple literal is an xsd:string literal; both spell the
                // same term and the bare form is canonical, which is what
                // this path has always written.
                None if l.datatype() == xsd::STRING => {}
                None => {
                    buf.push_str("^^<");
                    buf.push_str(l.datatype().as_str());
                    buf.push('>');
                }
            }
        }
        // RDF-star quoted triples: oxrdf only builds these with its rdf-12
        // feature, which is not enabled here.
        #[allow(unreachable_patterns)]
        other => write!(buf, "{other}").expect("formatting into a String cannot fail"),
    }
}

/// Enumerate the set-bit positions (term indices) of a bitset. Uses
/// `trailing_zeros` per word — far cheaper than iterating every bit and
/// calling `bit_test` (the old `externalize` pattern).
fn collect_set_indices(bitset: &Indices) -> Vec<u32> {
    // Estimate capacity from popcount to avoid Vec grow allocations.
    let popcount: usize = bitset.iter().flat_map(|block| block.iter()).map(|w| w.count_ones() as usize).sum();
    let mut out = Vec::with_capacity(popcount);
    for (block_idx, block) in bitset.iter().enumerate() {
        for (word_idx, &word) in block.iter().enumerate() {
            let base_bit = block_idx * 256 + word_idx * 64;
            let mut w = word;
            while w != 0 {
                let bit_offset = w.trailing_zeros() as usize;
                out.push(u32::try_from(base_bit + bit_offset).expect("term index overflow (>u32::MAX)"));
                w &= w - 1;
            }
        }
    }
    out
}

/// Build the four compressed dictionary sections and the three per-role
/// `index -> HDT id` lookup tables.
///
/// Sections follow the standard HDT MAPPING2 layout:
/// - shared: terms that appear as both subject and object (ids 1..=N_shared for both roles)
/// - unique subjects: subject-only terms (subject ids N_shared+1..=N_shared+N_subj)
/// - unique objects: object-only terms (object ids N_shared+1..=N_shared+N_obj)
/// - predicates: all predicate terms (ids 1..=N_pred)
fn build_dict_and_id_maps(
    terms: &Terms, subjects_bs: &Indices, predicates_bs: &Indices, objects_bs: &Indices, block_size: usize,
) -> (FourSectDict, IdMap, IdMap, IdMap) {
    use log::warn;

    if predicates_bs.is_empty() {
        warn!("no triples found in provided RDF");
    }

    // Compute section membership via bitset ops.
    let mut shared_bs = subjects_bs.clone();
    shared_bs.bit_and(objects_bs);
    let mut unique_subj_bs = subjects_bs.clone();
    unique_subj_bs.bit_andnot(objects_bs);
    let mut unique_obj_bs = objects_bs.clone();
    unique_obj_bs.bit_andnot(subjects_bs);

    // Collect the term indices in each section.
    let mut shared_keys = collect_set_indices(&shared_bs);
    let mut unique_subj_keys = collect_set_indices(&unique_subj_bs);
    let mut pred_keys = collect_set_indices(predicates_bs);
    let mut unique_obj_keys = collect_set_indices(&unique_obj_bs);
    drop(shared_bs);
    drop(unique_subj_bs);
    drop(unique_obj_bs);

    // Sort each section by the resolved string. Each `par_sort_unstable_by`
    // uses the rayon thread pool, so running the four sorts back-to-back lets
    // each one use every core; spawning them all in parallel would just fight
    // over the same workers.
    let cmp = |a: &u32, b: &u32| terms.cmp(*a, *b);
    shared_keys.par_sort_unstable_by(cmp);
    unique_subj_keys.par_sort_unstable_by(cmp);
    pred_keys.par_sort_unstable_by(cmp);
    unique_obj_keys.par_sort_unstable_by(cmp);

    // Allocate ID maps sized by the interner's term count (also the bit
    // length of the role bitsets).
    let map_len = terms.len();
    let mut subj_map: IdMap = vec![0u32; map_len];
    let mut pred_map: IdMap = vec![0u32; map_len];
    let mut obj_map: IdMap = vec![0u32; map_len];

    let n_shared = shared_keys.len();
    let shared_id_ceiling = u32::try_from(n_shared).expect("too many shared terms (>u32::MAX)");
    for (i, &key) in shared_keys.iter().enumerate() {
        let id = (i as u32) + 1; // ids are 1-indexed
        let slot = key as usize;
        subj_map[slot] = id;
        obj_map[slot] = id;
    }
    for (i, &key) in unique_subj_keys.iter().enumerate() {
        subj_map[key as usize] = shared_id_ceiling + (i as u32) + 1;
    }
    for (i, &key) in unique_obj_keys.iter().enumerate() {
        obj_map[key as usize] = shared_id_ceiling + (i as u32) + 1;
    }
    for (i, &key) in pred_keys.iter().enumerate() {
        pred_map[key as usize] = (i as u32) + 1;
    }

    // Compress the four sections concurrently. Each thread pulls its strings
    // straight from the term arena (no intermediate `Vec<&str>` or `BTreeSet`).
    let shared_ref = &shared_keys;
    let unique_subj_ref = &unique_subj_keys;
    let pred_ref = &pred_keys;
    let unique_obj_ref = &unique_obj_keys;
    let (shared, subjects, predicates, objects) = thread::scope(|s| {
        let h_shared = thread::Builder::new()
            .name("shared".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(shared_ref.iter().map(|&k| terms.get(k)), shared_ref.len(), block_size)
            })
            .unwrap();
        let h_subj = thread::Builder::new()
            .name("unique subjects".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(
                    unique_subj_ref.iter().map(|&k| terms.get(k)),
                    unique_subj_ref.len(),
                    block_size,
                )
            })
            .unwrap();
        let h_pred = thread::Builder::new()
            .name("predicates".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(pred_ref.iter().map(|&k| terms.get(k)), pred_ref.len(), block_size)
            })
            .unwrap();
        let h_obj = thread::Builder::new()
            .name("unique objects".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(
                    unique_obj_ref.iter().map(|&k| terms.get(k)),
                    unique_obj_ref.len(),
                    block_size,
                )
            })
            .unwrap();
        (h_shared.join().unwrap(), h_subj.join().unwrap(), h_pred.join().unwrap(), h_obj.join().unwrap())
    });

    (FourSectDict { shared, subjects, predicates, objects }, subj_map, pred_map, obj_map)
}

#[cfg(test)]
pub mod tests {
    use super::super::StringTriple;
    use super::super::tests::snikmeta_check;
    use super::{Hdt, HdtBuilder};
    use crate::hdt::tests::snikmeta;
    use crate::tests::init;
    use color_eyre::Result;
    use fs_err::File;
    use pretty_assertions::assert_eq;
    use std::io::{Cursor, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    /// Regression test for #131. Escapes are N-Triples syntax, not part of a term's value,
    /// so the dictionary must hold the decoded value and each serialization must escape it exactly once.
    #[test]
    fn read_nt_escapes() -> Result<()> {
        init();
        // tests/resources/escapes.nt in dictionary string format
        let want: Vec<StringTriple> = [
            ["_:b0", "urn:x:bnode", "\"x\""],
            ["urn:x:s", "urn:x:backslash", "\"back\\slash\"^^<urn:x:dt>"],
            ["urn:x:s", "urn:x:cr", "\"carriage\rreturn\""],
            ["urn:x:s", "urn:x:iri", "urn:x:oé"],
            ["urn:x:s", "urn:x:newline", "\"line\nbreak\"@en"],
            ["urn:x:s", "urn:x:nul1", "\"firsta\""],
            ["urn:x:s", "urn:x:nul4", "\"aa\""],
            ["urn:x:s", "urn:x:nul4", "\"ab\""],
            ["urn:x:s", "urn:x:plain", "\"nothing to escape\""],
            ["urn:x:s", "urn:x:quote", "\"say \"hi\"\""],
            ["urn:x:s", "urn:x:raw", "\"café 😀\""],
            ["urn:x:s", "urn:x:tab", "\"tab\there\""],
            ["urn:x:s", "urn:x:unicode", "\"café 😀\""],
        ]
        .map(|t| t.map(Arc::from))
        .into();

        let from_nt = Hdt::read_nt("tests/resources/escapes.nt")?;
        assert_eq!(from_nt.triples_all().collect::<Vec<_>>(), want, "dictionary must hold decoded values");

        // the same graph given as decoded strings must build the same HDT
        let mut wantmore = want.clone();
        wantmore.push(["urn:x:s", "urn:x:nul4", "\"a\u{0000}\"bc"].map(Arc::from));
        let from_triples = Hdt::from_triples(wantmore, "urn:x:escapes")?;
        assert_eq!(from_triples.triple_ids_with_pattern(Some("urn:x:s"), None, Some("\"aa\"")).count(), 1);
        assert_eq!(from_triples.triples_all().collect::<Vec<_>>(), want);
        assert_eq!(from_triples.triples.bitmap_y.inner().dict, from_nt.triples.bitmap_y.inner().dict);

        // the reported symptom: out to N-Triples and back in must not change a value
        fs_err::create_dir_all("tests/resources/generated")?;
        let path = Path::new("tests/resources/generated/escapes.nt");
        let mut writer = std::io::BufWriter::new(File::create(path)?);
        from_nt.write_nt(&mut writer)?;
        writer.flush()?;
        assert_eq!(Hdt::read_nt(path)?.triples_all().collect::<Vec<_>>(), want, "NT must escape once");

        let mut buf = Vec::<u8>::new();
        from_nt.write(&mut buf)?;
        let again = Hdt::read(Cursor::new(buf))?.triples_all().collect::<Vec<_>>();
        assert_eq!(again, want, "HDT must preserve values");
        Ok(())
    }

    #[test]
    fn read_nt() -> Result<()> {
        init();
        let path = Path::new("tests/resources/snikmeta.nt");
        if !path.exists() {
            log::info!("Creating test resource snikmeta.nt.");
            let mut writer = std::io::BufWriter::new(File::create(path)?);
            snikmeta()?.write_nt(&mut writer)?;
        }
        let invalid = path.join("doesnotexist");
        assert!(Hdt::read_nt(invalid).is_err(), "invalid N-Triples path should result in error");

        let snikmeta_nt = Hdt::read_nt(path)?;
        let snikmeta = snikmeta()?;
        let hdt_triples: Vec<StringTriple> = snikmeta.triples_all().collect();
        let nt_triples: Vec<StringTriple> = snikmeta_nt.triples_all().collect();

        assert_eq!(nt_triples, hdt_triples);
        assert_eq!(snikmeta.triples.bitmap_y.inner().dict, snikmeta_nt.triples.bitmap_y.inner().dict);
        snikmeta_check(&snikmeta_nt)?;
        let path = Path::new("tests/resources/empty.nt");
        let hdt_empty = Hdt::read_nt(path)?;
        let mut buf = Vec::<u8>::new();
        hdt_empty.write(&mut buf)?;
        Hdt::read(Cursor::new(buf))?;
        Ok(())
    }

    #[test]
    fn from_triples() -> Result<()> {
        init();
        let snikmeta = snikmeta()?;
        let triples: Vec<StringTriple> = snikmeta.triples_all().collect();
        let from_triples = Hdt::from_triples(triples, "http://www.snik.eu/ontology/meta")?;

        let hdt_triples: Vec<StringTriple> = snikmeta.triples_all().collect();
        let mem_triples: Vec<StringTriple> = from_triples.triples_all().collect();
        assert_eq!(mem_triples, hdt_triples);
        assert_eq!(snikmeta.triples.bitmap_y.inner().dict, from_triples.triples.bitmap_y.inner().dict);
        snikmeta_check(&from_triples)?;
        let mut buf = Vec::<u8>::new();
        from_triples.write(&mut buf)?;
        snikmeta_check(&Hdt::read(Cursor::new(buf))?)?;
        let hdt_empty = Hdt::from_triples(std::iter::empty::<[&str; 3]>(), "http://example.org/empty")?;
        let mut buf = Vec::<u8>::new();
        hdt_empty.write(&mut buf)?;
        Hdt::read(Cursor::new(buf))?;
        Ok(())
    }

    /// A scratch directory under the system temp dir, removed on drop.
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new(name: &str) -> Result<Self> {
            let dir = std::env::temp_dir().join(format!("hdt-nt-{name}-{}", std::process::id()));
            fs_err::create_dir_all(&dir)?;
            Ok(Self(dir))
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// snikmeta as N-Triples: all of it in `whole`, and split at a line
    /// boundary near the middle into `a` and `b`.
    struct SnikmetaNt {
        bytes: Vec<u8>,
        whole: PathBuf,
        a: PathBuf,
        b: PathBuf,
        /// The base IRI `read_nt` derives for `whole`.
        base: String,
    }

    fn snikmeta_nt(dir: &Path) -> Result<SnikmetaNt> {
        let mut bytes = Vec::new();
        snikmeta()?.write_nt(&mut bytes)?;
        let mid = bytes[..bytes.len() / 2].iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
        let [whole, a, b] = ["whole.nt", "a.nt", "b.nt"].map(|name| dir.join(name));
        fs_err::write(&whole, &bytes)?;
        fs_err::write(&a, &bytes[..mid])?;
        fs_err::write(&b, &bytes[mid..])?;
        let base = format!("file://{}", whole.canonicalize()?.display());
        Ok(SnikmetaNt { bytes, whole, a, b, base })
    }

    fn hdt_bytes(hdt: &Hdt) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        hdt.write(&mut buf)?;
        Ok(buf)
    }

    /// Loading snikmeta in pieces, through each kind of load, writes the same
    /// bytes as `read_nt` on the whole file: ids follow sorted strings, not load
    /// order, and the header has no timestamp.
    #[test]
    fn builder_matches_read_nt() -> Result<()> {
        init();
        let dir = ScratchDir::new("builder-matches")?;
        let nt = snikmeta_nt(&dir.0)?;
        let expected = hdt_bytes(&Hdt::read_nt(&nt.whole)?)?;

        let mut files = HdtBuilder::new();
        files.load_nt_file(&nt.a)?;
        files.load_nt_file(&nt.b)?;
        assert!(hdt_bytes(&files.finish(&nt.base)?)? == expected, "two N-Triples files");

        let parsed: Vec<oxrdf::Triple> =
            oxttl::NTriplesParser::new().for_slice(&nt.bytes).collect::<std::result::Result<_, _>>()?;
        let mut chunks = HdtBuilder::new();
        for chunk in parsed.chunks(parsed.len() / 3 + 1) {
            chunks.load_triples(chunk)?;
        }
        chunks.add_original_size(nt.bytes.len() as u64);
        assert!(hdt_bytes(&chunks.finish(&nt.base)?)? == expected, "parsed triples in chunks");

        // 100-byte blocks force many cuts, and lines longer than a block.
        let mut stream = HdtBuilder::new();
        stream.load_nt_blocks(nt.bytes.as_slice(), 100)?;
        assert!(hdt_bytes(&stream.finish(&nt.base)?)? == expected, "N-Triples stream");
        Ok(())
    }

    /// Every way of loading the escapes fixture stores the same decoded values
    /// and drops the same NUL-containing triples as `read_nt`.
    #[test]
    fn builder_matches_read_nt_escapes() -> Result<()> {
        init();
        let path = "tests/resources/escapes.nt";
        let want: Vec<StringTriple> = Hdt::read_nt(path)?.triples_all().collect();
        let bytes = fs_err::read(path)?;

        let mut stream = HdtBuilder::new();
        stream.load_nt_blocks(bytes.as_slice(), 16)?;
        assert_eq!(stream.finish("urn:x:escapes")?.triples_all().collect::<Vec<_>>(), want, "N-Triples stream");

        let parsed: Vec<oxrdf::Triple> =
            oxttl::NTriplesParser::new().for_slice(&bytes).collect::<std::result::Result<_, _>>()?;
        let mut triples = HdtBuilder::new();
        triples.load_triples(&parsed)?;
        assert_eq!(triples.finish("urn:x:escapes")?.triples_all().collect::<Vec<_>>(), want, "parsed triples");
        Ok(())
    }

    #[test]
    fn builder_removes_duplicate_triples() -> Result<()> {
        init();
        let dir = ScratchDir::new("builder-duplicates")?;
        let nt = snikmeta_nt(&dir.0)?;
        let once = Hdt::read_nt(&nt.whole)?;
        let mut twice = HdtBuilder::new();
        twice.load_nt_file(&nt.whole)?;
        twice.load_nt_file(&nt.whole)?;
        assert_eq!(twice.len(), 2 * once.triples_all().count());
        let twice = twice.finish(&nt.base)?;
        assert_eq!(twice.triples_all().collect::<Vec<_>>(), once.triples_all().collect::<Vec<_>>());
        Ok(())
    }

    /// A failed load keeps none of its triples, even though it interned some
    /// terms before reaching the bad line: the HDT matches one built without it.
    #[test]
    fn builder_failed_load_keeps_nothing() -> Result<()> {
        init();
        let dir = ScratchDir::new("builder-failed")?;
        let nt = snikmeta_nt(&dir.0)?;
        let mut bad_bytes = fs_err::read(&nt.b)?;
        bad_bytes.extend_from_slice(b"this is not N-Triples\n");
        let bad = dir.0.join("bad.nt");
        fs_err::write(&bad, &bad_bytes)?;

        let mut builder = HdtBuilder::new();
        builder.load_nt_file(&nt.a)?;
        let err = builder.load_nt_file(&bad).expect_err("malformed N-Triples file must fail");
        assert!(err.to_string().contains("bad.nt"), "error should name the file: {err}");
        assert!(builder.load_nt_reader(bad_bytes.as_slice()).is_err(), "malformed N-Triples stream must fail");

        let mut good = HdtBuilder::new();
        good.load_nt_file(&nt.a)?;
        assert!(hdt_bytes(&builder.finish(&nt.base)?)? == hdt_bytes(&good.finish(&nt.base)?)?);
        Ok(())
    }

    #[test]
    fn builder_empty() -> Result<()> {
        init();
        let mut builder = HdtBuilder::new();
        builder.load_triples(&[])?;
        assert_eq!(builder.len(), 0);
        let hdt = Hdt::read(Cursor::new(hdt_bytes(&builder.finish("http://example.org/empty")?)?))?;
        assert_eq!(hdt.triples_all().count(), 0);
        Ok(())
    }
}
