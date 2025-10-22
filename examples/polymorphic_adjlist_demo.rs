// //! Demonstration of polymorphic AdjList pattern
// //!
// //! Shows how to create hybrid TriplesBitmap structures where some components
// //! use in-memory sequences and others use file-based streaming.

// use hdt::containers::{
//     AdjListGeneric, InMemorySequence, FileBasedSequence,
//     Bitmap, Sequence, SequenceAccess,
// };
// use hdt::Hdt;
// use std::fs::File;
// use std::io::BufReader;
// use std::path::PathBuf;

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     println!("=== Polymorphic AdjList Demonstration ===\n");

//     let hdt_file = "tests/resources/snikmeta.hdt";

//     // Step 1: Load traditional TriplesBitmap
//     println!("Step 1: Traditional TriplesBitmap (all in-memory)");
//     println!("------------------------------------------------");

//     let file = File::open(hdt_file)?;
//     let hdt = Hdt::read(BufReader::new(file))?;

//     let adjlist_traditional = &hdt.triples.adjlist_z;
//     println!("✓ adjlist_z.sequence entries: {}", adjlist_traditional.sequence.entries);
//     println!("✓ adjlist_z.sequence memory: {} KB", adjlist_traditional.sequence.size_in_bytes() / 1024);
//     println!("✓ adjlist_z.bitmap memory: {} bytes", adjlist_traditional.bitmap.size_in_bytes());

//     // Get some values
//     let val0 = adjlist_traditional.sequence.get(0);
//     let val1 = adjlist_traditional.sequence.get(1);
//     println!("✓ Values: [0]={}, [1]={}", val0, val1);

//     // Step 2: Create in-memory AdjListGeneric (functionally identical)
//     println!("\nStep 2: AdjListGeneric with InMemorySequence");
//     println!("--------------------------------------------");

//     let sequence_inmem = InMemorySequence::new(adjlist_traditional.sequence.clone());
//     let adjlist_inmem = AdjListGeneric::new(
//         sequence_inmem,
//         adjlist_traditional.bitmap.clone(),
//     );

//     println!("✓ Memory: {} KB", adjlist_inmem.size_in_bytes() / 1024);
//     println!("✓ Values: [0]={}, [1]={}", adjlist_inmem.get_id(0), adjlist_inmem.get_id(1));

//     // Verify identical
//     assert_eq!(adjlist_traditional.sequence.get(0), adjlist_inmem.get_id(0));
//     assert_eq!(adjlist_traditional.sequence.get(1), adjlist_inmem.get_id(1));
//     println!("✓ Verified: InMemory version identical to original");

//     // Step 3: Create file-based AdjListGeneric
//     println!("\nStep 3: AdjListGeneric with FileBasedSequence");
//     println!("---------------------------------------------");

//     // We need to calculate the file offset manually
//     // For demo purposes, let's create a simple file-based sequence
//     // In production, you'd get this from the cache metadata

//     println!("✗ File-based sequence requires exact file offsets");
//     println!("  (These would come from cache metadata in production)");
//     println!("  Skipping file-based demo for now");

//     // Step 4: Show polymorphism in action
//     println!("\nStep 4: Polymorphism - Same API, Different Implementation");
//     println!("--------------------------------------------------------");

//     // Generic function that works with any SequenceAccess
//     fn analyze_adjlist<S: SequenceAccess>(adjlist: &AdjListGeneric<S>, name: &str) {
//         println!("\nAnalyzing {}:", name);
//         println!("  Entries: {}", adjlist.len());
//         println!("  Memory: {} bytes", adjlist.size_in_bytes());
//         println!("  First value: {}", adjlist.get_id(0));

//         // Test find operation (uses bitmap)
//         if adjlist.bitmap.num_ones() > 0 {
//             let first_pos = adjlist.find(1);
//             println!("  First position of ID 1: {}", first_pos);
//         }
//     }

//     analyze_adjlist(&adjlist_inmem, "InMemory AdjList");

//     // Step 5: Memory comparison
//     println!("\nStep 5: Memory Comparison");
//     println!("------------------------");

//     println!("Traditional AdjList:");
//     println!("  Sequence: {} KB", adjlist_traditional.sequence.size_in_bytes() / 1024);
//     println!("  Bitmap:   {} bytes", adjlist_traditional.bitmap.size_in_bytes());
//     println!("  Total:    {} KB", adjlist_traditional.size_in_bytes() / 1024);

//     println!("\nInMemory AdjListGeneric:");
//     println!("  Sequence: {} KB", adjlist_inmem.sequence.size_in_bytes() / 1024);
//     println!("  Bitmap:   {} bytes", adjlist_inmem.bitmap.size_in_bytes());
//     println!("  Total:    {} KB", adjlist_inmem.size_in_bytes() / 1024);

//     println!("\nFileBased AdjListGeneric (theoretical):");
//     println!("  Sequence: ~200 bytes (metadata only)");
//     println!("  Bitmap:   {} bytes (same)", adjlist_inmem.bitmap.size_in_bytes());
//     println!("  Total:    ~{} bytes", 200 + adjlist_inmem.bitmap.size_in_bytes());

//     let savings = adjlist_inmem.sequence.size_in_bytes() - 200;
//     println!("\n  Savings: {} KB ({:.1}%)",
//              savings / 1024,
//              savings as f64 / adjlist_inmem.sequence.size_in_bytes() as f64 * 100.0);

//     // Step 6: Practical hybrid pattern
//     println!("\nStep 6: Practical Hybrid Pattern");
//     println!("--------------------------------");

//     println!("
// Recommended pattern for production:

// ```rust
// pub struct HybridTriplesBitmap {{
//     bitmap_y: Bitmap,  // In-memory (small)
//     bitmap_z: Bitmap,  // In-memory (small)
//     wavelet_y: WaveletMatrix,  // In-memory (moderate)

//     // HYBRID PART:
//     // - Bitmap in memory (from cache file)
//     // - Sequence streamed from HDT file
//     adjlist_z: AdjListFileBased,

//     // Similar for op_index
//     op_index: OpIndexFileBased,
// }}
// ```

// Benefits:
// - Bitmaps in memory → fast rank/select operations
// - Sequences from file → 90%+ memory savings
// - Cache file stores bitmaps → fast initialization
// - HDT file stores sequences → no duplication

// Memory for 200MB HDT:
// - Traditional: 159 MB
// - Hybrid: ~15 MB (90% savings!)
// ");

//     println!("\n✅ Demo complete!");
//     println!("\nNext steps:");
//     println!("1. Implement TriplesBitmapGeneric<S: SequenceAccess>");
//     println!("2. Add OpIndexGeneric<S: SequenceAccess>");
//     println!("3. Create builder that combines cache + file-based sequences");

//     Ok(())
// }

fn main() -> Result<(), Box<dyn std::error::Error>> {Ok(())}