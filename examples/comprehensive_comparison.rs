use hdt::triples::{StreamingTriplesBitmap, StreamingIndexedTriplesBitmap, IndexConfig};
use hdt::Hdt;
use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

/// Comprehensive comparison of all three HDT access approaches:
/// 1. Traditional TriplesBitmap (full memory)
/// 2. StreamingTriplesBitmap (minimal memory)
/// 3. StreamingIndexedTriplesBitmap (configurable memory/performance trade-off)
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hdt_path = "tests/resources/tax-nodes.hdt";

    println!("HDT Implementation Comprehensive Comparison");
    println!("==========================================");

    // 1. Traditional HDT - Full Memory Approach
    println!("\n1. Traditional HDT (Full Memory Loading)");
    println!("----------------------------------------");

    let start = Instant::now();
    let file = File::open(hdt_path)?;
    let traditional_hdt = Hdt::read(BufReader::new(file))?;
    let traditional_load_time = start.elapsed();

    println!("✓ Load time: {:?}", traditional_load_time);
    println!("✓ Memory usage: {:.2} MB", traditional_hdt.size_in_bytes() as f64 / (1024.0 * 1024.0));
    println!("✓ Number of triples: {}", traditional_hdt.triples.adjlist_z.len());

    // 2. Basic Streaming HDT - Minimal Memory
    println!("\n2. Basic Streaming HDT (Minimal Memory)");
    println!("---------------------------------------");

    let start = Instant::now();
    let basic_streaming = StreamingTriplesBitmap::from_file(hdt_path)?;
    let basic_streaming_load_time = start.elapsed();

    println!("✓ Load time: {:?}", basic_streaming_load_time);
    println!("✓ Memory usage: {:.2} KB", basic_streaming.size_in_bytes() as f64 / 1024.0);
    println!("✓ Number of triples: {}", basic_streaming.num_triples());

    // 3. Indexed Streaming HDT - Configurable Approach
    println!("\n3. Indexed Streaming HDT (Configurable)");
    println!("---------------------------------------");

    // Test different memory configurations
    let configs = vec![
        ("Minimal (1KB budget)", IndexConfig {
            build_subject_index: false,
            build_predicate_index: false,
            build_object_index: false,
            max_index_memory: 1024,
            progressive_loading: true,
        }),
        ("Small (64KB budget)", IndexConfig {
            build_subject_index: true,
            build_predicate_index: false,
            build_object_index: false,
            max_index_memory: 64 * 1024,
            progressive_loading: true,
        }),
        ("Medium (1MB budget)", IndexConfig {
            build_subject_index: true,
            build_predicate_index: true,
            build_object_index: false,
            max_index_memory: 1024 * 1024,
            progressive_loading: true,
        }),
        ("Large (16MB budget)", IndexConfig {
            build_subject_index: true,
            build_predicate_index: true,
            build_object_index: true,
            max_index_memory: 16 * 1024 * 1024,
            progressive_loading: true,
        }),
    ];

    for (name, config) in configs {
        println!("\n   {} Configuration:", name);

        let start = Instant::now();
        let indexed_streaming = StreamingIndexedTriplesBitmap::from_file_with_config(hdt_path, config)?;
        let indexed_load_time = start.elapsed();

        println!("   ✓ Load time: {:?}", indexed_load_time);
        println!("   ✓ Index memory: {:.2} KB", indexed_streaming.index_memory_usage() as f64 / 1024.0);
        println!("   ✓ Number of triples: {}", indexed_streaming.num_triples());

        // Show which indexes were loaded
        print!("   ✓ Loaded indexes: ");
        let mut loaded = Vec::new();
        if indexed_streaming.indexes.subject_bitmap.is_some() {
            loaded.push("subject_bitmap");
        }
        if indexed_streaming.indexes.predicate_wavelet.is_some() {
            loaded.push("predicate_wavelet");
        }
        if indexed_streaming.indexes.object_index.is_some() {
            loaded.push("object_index");
        }
        if indexed_streaming.indexes.predicate_frequencies.is_some() {
            loaded.push("predicate_frequencies");
        }

        if loaded.is_empty() {
            println!("none (disk-only mode)");
        } else {
            println!("{}", loaded.join(", "));
        }
    }

    // Performance comparison for common operations
    println!("\n4. Performance Comparison");
    println!("------------------------");

    let test_positions = [0, 5, 10];
    let test_subjects = [1, 3, 5];

    // Set up indexed streaming with medium config for testing
    let indexed_streaming = StreamingIndexedTriplesBitmap::from_file_with_config(
        hdt_path,
        IndexConfig {
            build_subject_index: true,
            build_predicate_index: false,
            build_object_index: false,
            max_index_memory: 1024 * 1024,
            progressive_loading: true,
        }
    )?;

    println!("\n   Object Access Performance:");
    for &pos in &test_positions {
        // Traditional
        let start = Instant::now();
        let obj_traditional = traditional_hdt.triples.adjlist_z.sequence.get(pos);
        let traditional_time = start.elapsed();

        // Basic streaming
        let start = Instant::now();
        let obj_basic = basic_streaming.get_object(pos)?;
        let basic_time = start.elapsed();

        // Indexed streaming
        let start = Instant::now();
        let obj_indexed = indexed_streaming.get_object(pos)?;
        let indexed_time = start.elapsed();

        println!("   Position {}: Traditional({}) {:?}, Basic({}) {:?}, Indexed({}) {:?}",
                 pos, obj_traditional, traditional_time, obj_basic, basic_time, obj_indexed, indexed_time);
    }

    println!("\n   Subject Lookup Performance:");
    for &subject_id in &test_subjects {
        // Traditional
        let start = Instant::now();
        let pos_traditional = traditional_hdt.triples.find_y(subject_id);
        let traditional_time = start.elapsed();

        // Basic streaming
        let start = Instant::now();
        let pos_basic = basic_streaming.find_y(subject_id)?;
        let basic_time = start.elapsed();

        // Indexed streaming
        let start = Instant::now();
        let pos_indexed = indexed_streaming.find_y(subject_id)?;
        let indexed_time = start.elapsed();

        println!("   Subject {}: Traditional({}) {:?}, Basic({}) {:?}, Indexed({}) {:?}",
                 subject_id, pos_traditional, traditional_time, pos_basic, basic_time, pos_indexed, indexed_time);
    }

    // Memory usage summary
    println!("\n5. Memory Usage Summary");
    println!("----------------------");

    let traditional_mb = traditional_hdt.size_in_bytes() as f64 / (1024.0 * 1024.0);
    let basic_kb = basic_streaming.size_in_bytes() as f64 / 1024.0;
    let indexed_kb = indexed_streaming.index_memory_usage() as f64 / 1024.0;

    println!("Traditional HDT:      {:.2} MB (100%)", traditional_mb);
    println!("Basic Streaming:      {:.2} KB ({:.1}%)", basic_kb, basic_kb / 1024.0 / traditional_mb * 100.0);
    println!("Indexed Streaming:    {:.2} KB ({:.1}%)", indexed_kb, indexed_kb / 1024.0 / traditional_mb * 100.0);

    println!("\nMemory savings vs Traditional:");
    println!("Basic Streaming:      {:.1}x less memory", traditional_mb * 1024.0 / basic_kb);
    println!("Indexed Streaming:    {:.1}x less memory", traditional_mb * 1024.0 / indexed_kb.max(1.0));

    // Recommendations
    println!("\n6. Usage Recommendations");
    println!("------------------------");

    println!("📊 Traditional HDT:");
    println!("   ✅ Use when: Plenty of memory, frequent queries, maximum performance needed");
    println!("   ❌ Avoid when: Memory constrained, large files, many concurrent processes");

    println!("\n🔄 Basic Streaming HDT:");
    println!("   ✅ Use when: Very limited memory, infrequent queries, exploration tasks");
    println!("   ❌ Avoid when: Need fast query performance, doing many repeated queries");

    println!("\n⚖️ Indexed Streaming HDT:");
    println!("   ✅ Use when: Moderate memory constraints, mixed workloads, production systems");
    println!("   ✅ Best for: Balancing memory usage with performance requirements");
    println!("   ✅ Configurable: Tune index selection based on query patterns and memory budget");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_implementations_consistency() -> Result<(), Box<dyn std::error::Error>> {
        // Test that all implementations return consistent results
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let traditional = Hdt::read(BufReader::new(file))?;

        let basic_streaming = StreamingTriplesBitmap::from_file("tests/resources/snikmeta.hdt")?;

        let indexed_streaming = StreamingIndexedTriplesBitmap::from_file_with_config(
            "tests/resources/snikmeta.hdt",
            IndexConfig::default()
        )?;

        // All should report same number of triples
        assert_eq!(traditional.triples.adjlist_z.len(), basic_streaming.num_triples());
        assert_eq!(traditional.triples.adjlist_z.len(), indexed_streaming.num_triples());

        // Test object access consistency
        for pos in 0..5.min(traditional.triples.adjlist_z.len()) {
            let obj_traditional = traditional.triples.adjlist_z.sequence.get(pos);
            let obj_basic = basic_streaming.get_object(pos)?;
            let obj_indexed = indexed_streaming.get_object(pos)?;

            assert_eq!(obj_traditional, obj_basic, "Basic streaming mismatch at position {}", pos);
            assert_eq!(obj_traditional, obj_indexed, "Indexed streaming mismatch at position {}", pos);
        }

        Ok(())
    }

    #[test]
    fn test_memory_scaling() -> Result<(), Box<dyn std::error::Error>> {
        // Test that memory usage scales with index configuration
        let configs = [
            (1024, false, false, false),          // 1KB - minimal
            (64 * 1024, true, false, false),      // 64KB - subject only
            (1024 * 1024, true, true, false),     // 1MB - subject + predicate
            (16 * 1024 * 1024, true, true, true), // 16MB - all indexes
        ];

        let mut prev_memory = 0;

        for (budget, subject, predicate, object) in configs {
            let config = IndexConfig {
                build_subject_index: subject,
                build_predicate_index: predicate,
                build_object_index: object,
                max_index_memory: budget,
                progressive_loading: true,
            };

            let streaming = StreamingIndexedTriplesBitmap::from_file_with_config(
                "tests/resources/snikmeta.hdt",
                config
            )?;

            let memory_usage = streaming.index_memory_usage();

            // Memory usage should generally increase with more indexes
            // (unless we hit memory limits)
            if budget > 1024 {  // Skip the minimal case
                assert!(memory_usage >= prev_memory,
                       "Memory usage decreased: {} -> {}", prev_memory, memory_usage);
            }

            assert!(memory_usage <= budget,
                   "Memory usage {} exceeded budget {}", memory_usage, budget);

            prev_memory = memory_usage;
        }

        Ok(())
    }
}