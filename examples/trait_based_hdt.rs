// use hdt::{create_hdt, TripleAccessStrategy, IndexConfig, HdtTrait};
// use std::time::Instant;

// /// Example demonstrating the trait-based approach to HDT access.
// /// Shows how to use the generic HDT interface that works with any triple access implementation.
// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let hdt_path = "tests/resources/tax-nodes.hdt";

//     println!("HDT Trait-Based Interface Demonstration");
//     println!("=======================================");

//     // Create different HDT implementations using the factory function
//     let strategies = vec![
//         ("Traditional (Full Memory)", TripleAccessStrategy::FullMemory),
//         ("Basic Streaming (Minimal Memory)", TripleAccessStrategy::Streaming),
//         ("Indexed Streaming (Balanced)", TripleAccessStrategy::IndexedStreaming(IndexConfig::default())),
//         ("Indexed Streaming (Memory Constrained)", TripleAccessStrategy::IndexedStreaming(IndexConfig {
//             build_subject_index: true,
//             build_predicate_index: false,
//             build_object_index: false,
//             max_index_memory: 64 * 1024, // 64KB limit
//             progressive_loading: true,
//         })),
//     ];

//     println!("\n1. Creating HDT instances with different strategies:");
//     println!("--------------------------------------------------");

//     let mut hdt_instances: Vec<(String, Box<dyn HdtTrait>)> = Vec::new();

//     for (name, strategy) in strategies {
//         println!("\n   Creating {}", name);
//         let start = Instant::now();

//         match create_hdt(hdt_path, strategy) {
//             Ok(hdt) => {
//                 let load_time = start.elapsed();
//                 println!("   ✓ Loaded in {:?}", load_time);
//                 println!("   ✓ Memory usage: {:.2} KB", hdt.size_in_bytes() as f64 / 1024.0);
//                 println!("   ✓ Number of triples: {}", hdt.num_triples());

//                 hdt_instances.push((name.to_string(), hdt));
//             }
//             Err(e) => {
//                 println!("   ✗ Failed to load: {}", e);
//             }
//         }
//     }

//     // Demonstrate polymorphic usage
//     println!("\n2. Polymorphic Operations (same interface, different implementations):");
//     println!("-------------------------------------------------------------------");

//     for (name, hdt) in &hdt_instances {
//         println!("\n   {} Statistics:", name);
//         println!("   - Triples: {}", hdt.num_triples());
//         println!("   - Memory: {:.2} KB", hdt.size_in_bytes() as f64 / 1024.0);

//         // Demonstrate searching (simplified example)
//         let search_pattern = [1, 0, 0]; // Subject 1, any predicate, any object
//         let start = Instant::now();
//         match hdt.search_triples(search_pattern) {
//             Ok(results) => {
//                 let search_time = start.elapsed();
//                 println!("   - Search S?? (subject 1): {} results in {:?}", results.len(), search_time);
//             }
//             Err(e) => {
//                 println!("   - Search failed: {}", e);
//             }
//         }
//     }

//     // Demonstrate string translation
//     println!("\n3. String Translation Example:");
//     println!("-----------------------------");

//     if let Some((name, hdt)) = hdt_instances.first() {
//         println!("\n   Using {} for translation example:", name);

//         // Try to translate a few triples to strings
//         for triple_id in [[1, 1, 1], [2, 1, 2], [3, 2, 3]] {
//             match hdt.translate_triple(triple_id) {
//                 Ok(string_triple) => {
//                     println!("   Triple {:?} = [{}, {}, {}]",
//                              triple_id, string_triple[0], string_triple[1], string_triple[2]);
//                 }
//                 Err(e) => {
//                     println!("   Could not translate {:?}: {}", triple_id, e);
//                 }
//             }
//         }
//     }

//     // Memory usage comparison
//     println!("\n4. Memory Usage Comparison:");
//     println!("---------------------------");

//     if hdt_instances.len() > 1 {
//         let traditional_memory = hdt_instances[0].1.size_in_bytes();

//         for (name, hdt) in &hdt_instances[1..] {
//             let memory = hdt.size_in_bytes();
//             let ratio = traditional_memory as f64 / memory as f64;
//             println!("   {} uses {:.1}x less memory than traditional", name, ratio);
//         }
//     }

//     // Function that works with any HDT implementation
//     fn analyze_hdt(name: &str, hdt: &dyn HdtTrait) {
//         println!("\n   Analyzing {}:", name);
//         println!("   - Total triples: {}", hdt.num_triples());
//         println!("   - Memory footprint: {:.2} MB", hdt.size_in_bytes() as f64 / (1024.0 * 1024.0));

//         // This function doesn't need to know which implementation it's working with
//         let density = hdt.num_triples() as f64 / hdt.size_in_bytes() as f64 * 1024.0;
//         println!("   - Triples per KB: {:.1}", density);
//     }

//     println!("\n5. Generic Analysis Function:");
//     println!("-----------------------------");

//     for (name, hdt) in &hdt_instances {
//         analyze_hdt(name, hdt.as_ref());
//     }

//     println!("\n6. Key Benefits of the Trait-Based Approach:");
//     println!("--------------------------------------------");

//     println!("   ✅ **Polymorphism**: Same interface works with all implementations");
//     println!("   ✅ **Flexibility**: Choose strategy based on requirements");
//     println!("   ✅ **Composability**: Easy to add new triple access methods");
//     println!("   ✅ **Performance Tuning**: Can switch strategies without changing client code");
//     println!("   ✅ **Memory Management**: Fine-grained control over memory vs performance trade-offs");

//     println!("\n7. Usage Patterns:");
//     println!("------------------");

//     println!("   🔧 **Development/Testing**: Use traditional for speed");
//     println!("   🚀 **Production (High Memory)**: Use traditional for performance");
//     println!("   📱 **Production (Low Memory)**: Use streaming for efficiency");
//     println!("   ⚖️ **Production (Balanced)**: Use indexed streaming with appropriate config");
//     println!("   🔄 **Multi-Tenant**: Different strategies per tenant based on data size");

//     Ok(())
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use hdt::{GenericHdt, TriplesBitmap, StreamingTriplesBitmap, StreamingIndexedTriplesBitmap};
//     use std::fs::File;
//     use std::io::BufReader;

//     #[test]
//     fn test_trait_object_usage() -> Result<(), Box<dyn std::error::Error>> {
//         // Test that we can use trait objects effectively
//         let traditional = create_hdt("tests/resources/snikmeta.hdt", TripleAccessStrategy::FullMemory)?;
//         let streaming = create_hdt("tests/resources/snikmeta.hdt", TripleAccessStrategy::Streaming)?;

//         // Both should implement the same trait
//         assert_eq!(traditional.num_triples(), streaming.num_triples());
//         assert!(streaming.size_in_bytes() < traditional.size_in_bytes());

//         Ok(())
//     }

//     #[test]
//     fn test_direct_generic_usage() -> Result<(), Box<dyn std::error::Error>> {
//         // Test using the generic types directly (without trait objects)

//         // Traditional
//         let file = File::open("tests/resources/snikmeta.hdt")?;
//         let reader = BufReader::new(file);
//         let traditional_hdt = GenericHdt::<TriplesBitmap>::read_traditional(reader)?;

//         // Streaming
//         let streaming_hdt = GenericHdt::<StreamingTriplesBitmap>::read_streaming("tests/resources/snikmeta.hdt")?;

//         // Indexed Streaming
//         let indexed_hdt = GenericHdt::<StreamingIndexedTriplesBitmap>::read_indexed_streaming_default("tests/resources/snikmeta.hdt")?;

//         // All should have same number of triples
//         assert_eq!(traditional_hdt.num_triples(), streaming_hdt.num_triples());
//         assert_eq!(traditional_hdt.num_triples(), indexed_hdt.num_triples());

//         // Memory usage should be different
//         assert!(streaming_hdt.size_in_bytes() < traditional_hdt.size_in_bytes());
//         assert!(indexed_hdt.size_in_bytes() < traditional_hdt.size_in_bytes());

//         Ok(())
//     }

//     #[test]
//     fn test_polymorphic_function() -> Result<(), Box<dyn std::error::Error>> {
//         fn count_triples(hdt: &dyn HdtTrait) -> usize {
//             hdt.num_triples()
//         }

//         let traditional = create_hdt("tests/resources/snikmeta.hdt", TripleAccessStrategy::FullMemory)?;
//         let streaming = create_hdt("tests/resources/snikmeta.hdt", TripleAccessStrategy::Streaming)?;

//         assert_eq!(count_triples(traditional.as_ref()), count_triples(streaming.as_ref()));

//         Ok(())
//     }
// }

fn main() -> Result<(), Box<dyn std::error::Error>> {Ok(())}