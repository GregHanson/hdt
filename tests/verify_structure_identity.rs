// //! Test to verify that bitmap/wavelet structures are identical
// //! between TriplesBitmap and HybridTripleAccess

// use hdt::triples::{TriplesBitmap, HybridTripleAccess};
// use hdt::Hdt;
// use std::fs::File;
// use std::io::BufReader;
// use sucds::Serializable;

// #[test]
// fn test_structure_identity() -> Result<(), Box<dyn std::error::Error>> {
//     let test_file = "tests/resources/snikmeta.hdt";

//     // Load TriplesBitmap
//     let file = File::open(test_file)?;
//     let hdt = Hdt::read(BufReader::new(file))?;
//     let triples_bitmap = &hdt.triples;

//     // Load HybridTripleAccess
//     let hybrid = HybridTripleAccess::from_file(test_file)?;

//     println!("=== Comparing Structures ===");

//     // Compare bitmap_y
//     println!("\n1. Comparing bitmap_y:");
//     println!("  TriplesBitmap bitmap_y.len(): {}", triples_bitmap.bitmap_y.len());
//     println!("  HybridAccess bitmap_y.len():  {}", hybrid.bitmap_y.len());
//     assert_eq!(triples_bitmap.bitmap_y.len(), hybrid.bitmap_y.len());

//     println!("  TriplesBitmap bitmap_y.num_ones(): {}", triples_bitmap.bitmap_y.num_ones());
//     println!("  HybridAccess bitmap_y.num_ones():  {}", hybrid.bitmap_y.num_ones());
//     assert_eq!(triples_bitmap.bitmap_y.num_ones(), hybrid.bitmap_y.num_ones());

//     // Compare select1 results (should be identical)
//     for i in 0..triples_bitmap.bitmap_y.num_ones().min(5) {
//         let pos_bitmap = triples_bitmap.bitmap_y.select1(i);
//         let pos_hybrid = hybrid.bitmap_y.select1(i);
//         println!("  select1({}) -> TriplesBitmap: {:?}, Hybrid: {:?}", i, pos_bitmap, pos_hybrid);
//         assert_eq!(pos_bitmap, pos_hybrid);
//     }

//     // Compare bitmap_z (adjlist_z.bitmap)
//     println!("\n2. Comparing bitmap_z (adjlist_z.bitmap):");
//     println!("  TriplesBitmap bitmap_z.len(): {}", triples_bitmap.adjlist_z.bitmap.len());
//     println!("  HybridAccess bitmap_z.len():  {}", hybrid.bitmap_z.len());
//     assert_eq!(triples_bitmap.adjlist_z.bitmap.len(), hybrid.bitmap_z.len());

//     println!("  TriplesBitmap bitmap_z.num_ones(): {}", triples_bitmap.adjlist_z.bitmap.num_ones());
//     println!("  HybridAccess bitmap_z.num_ones():  {}", hybrid.bitmap_z.num_ones());
//     assert_eq!(triples_bitmap.adjlist_z.bitmap.num_ones(), hybrid.bitmap_z.num_ones());

//     // Compare wavelet_y
//     println!("\n3. Comparing wavelet_y:");
//     println!("  TriplesBitmap wavelet_y.len(): {}", triples_bitmap.wavelet_y.len());
//     println!("  HybridAccess wavelet_y.len():  {}", hybrid.wavelet_y.len());
//     assert_eq!(triples_bitmap.wavelet_y.len(), hybrid.wavelet_y.len());

//     // Compare wavelet access (should return same predicate IDs)
//     for i in 0..triples_bitmap.wavelet_y.len().min(10) {
//         let pred_bitmap = triples_bitmap.wavelet_y.access(i).unwrap();
//         let pred_hybrid = hybrid.wavelet_y.access(i).unwrap();
//         println!("  wavelet_y.access({}) -> TriplesBitmap: {}, Hybrid: {}", i, pred_bitmap, pred_hybrid);
//         assert_eq!(pred_bitmap, pred_hybrid);
//     }

//     // Serialize and compare byte representations
//     println!("\n4. Comparing serialized representations:");

//     let mut bitmap_y_bytes_triples = Vec::new();
//     triples_bitmap.bitmap_y.dict.serialize_into(&mut bitmap_y_bytes_triples)?;

//     let mut bitmap_y_bytes_hybrid = Vec::new();
//     hybrid.bitmap_y.dict.serialize_into(&mut bitmap_y_bytes_hybrid)?;

//     println!("  bitmap_y serialized size (TriplesBitmap): {} bytes", bitmap_y_bytes_triples.len());
//     println!("  bitmap_y serialized size (Hybrid):        {} bytes", bitmap_y_bytes_hybrid.len());
//     assert_eq!(bitmap_y_bytes_triples.len(), bitmap_y_bytes_hybrid.len());
//     assert_eq!(bitmap_y_bytes_triples, bitmap_y_bytes_hybrid, "bitmap_y serializations should be identical!");

//     let mut wavelet_bytes_triples = Vec::new();
//     triples_bitmap.wavelet_y.serialize_into(&mut wavelet_bytes_triples)?;

//     let mut wavelet_bytes_hybrid = Vec::new();
//     hybrid.wavelet_y.serialize_into(&mut wavelet_bytes_hybrid)?;

//     println!("  wavelet_y serialized size (TriplesBitmap): {} bytes", wavelet_bytes_triples.len());
//     println!("  wavelet_y serialized size (Hybrid):        {} bytes", wavelet_bytes_hybrid.len());
//     assert_eq!(wavelet_bytes_triples.len(), wavelet_bytes_hybrid.len());
//     assert_eq!(wavelet_bytes_triples, wavelet_bytes_hybrid, "wavelet_y serializations should be identical!");

//     println!("\n✅ All structures are IDENTICAL!");
//     println!("   This confirms we can use a prebuilt cache file.");

//     Ok(())
// }
