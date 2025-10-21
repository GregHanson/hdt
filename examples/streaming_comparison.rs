// use hdt::triples::StreamingTriplesBitmap;
// use hdt::Hdt;
// use std::fs::File;
// use std::io::BufReader;
// use std::time::Instant;

// /// Example demonstrating the difference between the traditional TriplesBitmap
// /// that loads everything into memory vs the new StreamingTriplesBitmap that
// /// scans the file at query time.
// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Path to an HDT file - update this to point to your HDT file
//     let hdt_path = "tests/resources/tax-nodes.hdt";

//     println!("HDT Streaming vs Memory-based Comparison");
//     println!("========================================");

//     // Load traditional HDT (loads everything into memory)
//     println!("\n1. Loading traditional HDT (all data in memory)...");
//     let start = Instant::now();
//     let file = File::open(hdt_path)?;
//     let hdt = Hdt::read(BufReader::new(file))?;
//     let load_time = start.elapsed();

//     println!("   ✓ Loaded in {:?}", load_time);
//     println!("   ✓ Memory usage: {:.2} MB", hdt.size_in_bytes() as f64 / (1024.0 * 1024.0));
//     println!("   ✓ Number of triples: {}", hdt.triples.adjlist_z.len());

//     // Load streaming HDT (minimal memory usage)
//     println!("\n2. Loading streaming HDT (file-based scanning)...");
//     let start = Instant::now();
//     let streaming_hdt = StreamingTriplesBitmap::from_file(hdt_path)?;
//     let streaming_load_time = start.elapsed();

//     println!("   ✓ Loaded in {:?}", streaming_load_time);
//     println!("   ✓ Memory usage: {:.2} KB", streaming_hdt.size_in_bytes() as f64 / 1024.0);
//     println!("   ✓ Number of triples: {}", streaming_hdt.num_triples());

//     // Compare memory usage
//     let memory_ratio = hdt.size_in_bytes() as f64 / streaming_hdt.size_in_bytes() as f64;
//     println!("\n3. Memory Usage Comparison:");
//     println!("   ✓ Traditional HDT uses {:.1}x more memory", memory_ratio);
//     println!("   ✓ Memory saved: {:.2} MB",
//              (hdt.size_in_bytes() - streaming_hdt.size_in_bytes()) as f64 / (1024.0 * 1024.0));

//     // Performance comparison for basic operations
//     println!("\n4. Performance Comparison:");

//     // Test getting a specific object
//     let pos_z = 10; // Example position

//     // Traditional approach
//     let start = Instant::now();
//     let obj_traditional = hdt.triples.adjlist_z.sequence.get(pos_z);
//     let traditional_time = start.elapsed();

//     // Streaming approach
//     let start = Instant::now();
//     let obj_streaming = streaming_hdt.get_object(pos_z)?;
//     let streaming_time = start.elapsed();

//     println!("   ✓ Getting object at position {}:", pos_z);
//     println!("     - Traditional: {} (took {:?})", obj_traditional, traditional_time);
//     println!("     - Streaming:   {} (took {:?})", obj_streaming, streaming_time);

//     // Test subject lookup
//     let subject_id = 5;

//     let start = Instant::now();
//     let pos_y_traditional = hdt.triples.find_y(subject_id);
//     let traditional_lookup_time = start.elapsed();

//     let start = Instant::now();
//     let pos_y_streaming = streaming_hdt.find_y(subject_id)?;
//     let streaming_lookup_time = start.elapsed();

//     println!("   ✓ Finding Y position for subject {}:", subject_id);
//     println!("     - Traditional: {} (took {:?})", pos_y_traditional, traditional_lookup_time);
//     println!("     - Streaming:   {} (took {:?})", pos_y_streaming, streaming_lookup_time);

//     println!("\n5. Use Case Recommendations:");
//     println!("   ✓ Use Traditional HDT when:");
//     println!("     - You have plenty of memory available");
//     println!("     - You need maximum query performance");
//     println!("     - You'll be doing many repeated queries");

//     println!("   ✓ Use Streaming HDT when:");
//     println!("     - Memory is limited (large HDT files, many concurrent processes)");
//     println!("     - You only need occasional queries");
//     println!("     - You're processing very large HDT files (>RAM size)");
//     println!("     - You need to minimize startup time");

//     Ok(())
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_streaming_hdt_basic() -> Result<(), Box<dyn std::error::Error>> {
//         let streaming_hdt = StreamingTriplesBitmap::from_file("tests/resources/snik.hdt")?;

//         // Test basic metadata access
//         assert!(streaming_hdt.num_triples() > 0);
//         assert!(streaming_hdt.size_in_bytes() > 0);

//         // Test object access
//         let obj = streaming_hdt.get_object(0)?;
//         assert!(obj > 0);

//         Ok(())
//     }

//     #[test]
//     fn test_memory_comparison() -> Result<(), Box<dyn std::error::Error>> {
//         // Load both versions
//         let file = File::open("tests/resources/snikmeta.hdt")?;
//         let hdt = Hdt::read(BufReader::new(file))?;
//         let streaming_hdt = StreamingTriplesBitmap::from_file("tests/resources/snikmeta.hdt")?;

//         // Streaming version should use significantly less memory
//         assert!(streaming_hdt.size_in_bytes() < hdt.size_in_bytes() / 10);

//         // Both should report same number of triples
//         assert_eq!(hdt.triples.adjlist_z.len(), streaming_hdt.num_triples());

//         Ok(())
//     }
// }

fn main() -> Result<(), Box<dyn std::error::Error>> {Ok(())}