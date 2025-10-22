// //! Example demonstrating HybridTripleAccess with cache files
// //!
// //! Shows how to:
// //! 1. Generate a cache file from TriplesBitmap
// //! 2. Load HybridTripleAccess with automatic cache detection
// //! 3. Measure performance improvements

// use hdt::triples::{HybridTripleAccess, TripleAccess};
// use hdt::Hdt;
// use std::fs::File;
// use std::io::BufReader;
// use std::time::Instant;

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let hdt_file = "tests/resources/tax-nodes.hdt";
//     let cache_file = "/tmp/tax-nodes.hdt.cache";

//     println!("=== HybridTripleAccess Cache Demo ===\n");

//     // Step 1: Generate cache from TriplesBitmap (one-time operation)
//     println!("Step 1: Generating cache file from TriplesBitmap");
//     println!("-----------------------------------------------");

//     let start = Instant::now();
//     let file = File::open(hdt_file)?;
//     let hdt = Hdt::read(BufReader::new(file))?;
//     let load_time = start.elapsed();

//     println!("✓ Loaded TriplesBitmap in {:?}", load_time);
//     println!("  Memory: {:.2} MB", hdt.triples.size_in_bytes() as f64 / 1024.0 / 1024.0);

//     // Generate cache
//     let start = Instant::now();
//     HybridTripleAccess::generate_cache_from_triples(&hdt.triples, hdt_file, cache_file)?;
//     let cache_gen_time = start.elapsed();

//     let cache_size = std::fs::metadata(cache_file)?.len();
//     println!("✓ Generated cache in {:?}", cache_gen_time);
//     println!("  Cache file size: {} bytes ({:.2} KB)", cache_size, cache_size as f64 / 1024.0);

//     // Step 2: Load without cache
//     println!("\nStep 2: Loading HybridTripleAccess WITHOUT cache");
//     println!("-----------------------------------------------");

//     // Remove cache to test without it
//     std::fs::remove_file(cache_file)?;

//     let start = Instant::now();
//     let hybrid_no_cache = HybridTripleAccess::from_file(hdt_file)?;
//     let load_no_cache_time = start.elapsed();

//     println!("✓ Loaded in {:?}", load_no_cache_time);
//     println!("  Memory: {:.2} KB", hybrid_no_cache.size_in_bytes() as f64 / 1024.0);
//     println!("  Triples: {}", hybrid_no_cache.num_triples());

//     // Step 3: Regenerate cache and load WITH cache
//     println!("\nStep 3: Loading HybridTripleAccess WITH cache");
//     println!("--------------------------------------------");

//     // Regenerate cache
//     HybridTripleAccess::generate_cache_from_triples(&hdt.triples, hdt_file, cache_file)?;

//     let start = Instant::now();
//     let hybrid_with_cache = HybridTripleAccess::from_file(hdt_file)?;
//     let load_with_cache_time = start.elapsed();

//     println!("✓ Loaded in {:?}", load_with_cache_time);
//     println!("  Memory: {:.2} KB", hybrid_with_cache.size_in_bytes() as f64 / 1024.0);
//     println!("  Triples: {}", hybrid_with_cache.num_triples());

//     // Step 4: Performance comparison
//     println!("\nStep 4: Performance Comparison");
//     println!("------------------------------");

//     let speedup = load_no_cache_time.as_micros() as f64 / load_with_cache_time.as_micros() as f64;

//     println!("Load time without cache: {:?}", load_no_cache_time);
//     println!("Load time with cache:    {:?}", load_with_cache_time);
//     println!("Speedup:                 {:.2}x faster", speedup);

//     println!("\nMemory comparison:");
//     println!("TriplesBitmap:     {:.2} MB (100%)", hdt.triples.size_in_bytes() as f64 / 1024.0 / 1024.0);
//     println!("HybridTripleAccess: {:.2} KB ({:.1}%)",
//              hybrid_with_cache.size_in_bytes() as f64 / 1024.0,
//              hybrid_with_cache.size_in_bytes() as f64 / hdt.triples.size_in_bytes() as f64 * 100.0);

//     // Step 5: Verify functionality
//     println!("\nStep 5: Verifying cache-loaded data");
//     println!("-----------------------------------");

//     // Test a few queries to ensure cache loaded correctly
//     for subject_id in 1..=5 {
//         let pos_triples = hdt.triples.find_y(subject_id);
//         let pos_hybrid = hybrid_with_cache.find_y(subject_id)?;

//         print!("Subject {}: TriplesBitmap={}, HybridCache={}", subject_id, pos_triples, pos_hybrid);

//         if pos_triples == pos_hybrid {
//             println!(" ✓");
//         } else {
//             println!(" ✗ MISMATCH!");
//             return Err("Cache data doesn't match original!".into());
//         }
//     }

//     // Test predicate access
//     for pos in 0..5 {
//         let pred_triples = hdt.triples.wavelet_y.access(pos).unwrap();
//         let pred_hybrid = hybrid_with_cache.get_predicate(pos)?;

//         print!("Predicate[{}]: TriplesBitmap={}, HybridCache={}", pos, pred_triples, pred_hybrid);

//         if pred_triples == pred_hybrid {
//             println!(" ✓");
//         } else {
//             println!(" ✗ MISMATCH!");
//             return Err("Cache data doesn't match original!".into());
//         }
//     }

//     println!("\n✅ All verification tests passed!");

//     // Step 6: Deployment recommendations
//     println!("\nStep 6: Deployment Recommendations");
//     println!("----------------------------------");

//     println!("
// For production deployments:

// 1. Pre-build cache files:
//    ```bash
//    # In your deployment pipeline
//    cargo run --example hybrid_with_cache -- --generate-cache data.hdt
//    ```

// 2. Deploy both files:
//    - data.hdt (original HDT file)
//    - data.hdt.cache (prebuilt index cache)

// 3. Application code automatically uses cache:
//    ```rust
//    // Automatically detects and uses .cache file
//    let triples = HybridTripleAccess::from_file(\"data.hdt\")?;
//    ```

// 4. Benefits:
//    - {:.2}x faster initialization
//    - {} bytes cache file (tiny!)
//    - Identical query results
//    - Automatic fallback if cache missing/corrupt

// 5. Cache file lifetime:
//    - Regenerate when HDT file changes
//    - Cache is version-specific (format v{})
// ", speedup, cache_size, 1);

//     // Cleanup
//     std::fs::remove_file(cache_file)?;

//     println!("\n✅ Demo complete!");

//     Ok(())
// }

fn main() -> Result<(), Box<dyn std::error::Error>> {Ok(())}
