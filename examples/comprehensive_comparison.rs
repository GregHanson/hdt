use hdt::triples::{StreamingTriplesBitmap, StreamingIndexedTriplesBitmap, FileBasedTripleAccess, IndexConfig, TripleAccess};
use hdt::Hdt;
use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

/// Comprehensive comparison of all four HDT access approaches:
/// 1. Traditional TriplesBitmap (full memory - fastest)
/// 2. StreamingIndexedTriplesBitmap (configurable memory/performance trade-off)
/// 3. StreamingTriplesBitmap (minimal memory with basic streaming)
/// 4. FileBasedTripleAccess (zero-index pure file I/O - lowest memory)
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

    // 3. File-Based HDT - Zero-Index Pure File I/O
    println!("\n3. File-Based HDT (Zero-Index, Pure File I/O)");
    println!("---------------------------------------------");

    let start = Instant::now();
    let file_based = FileBasedTripleAccess::from_file(hdt_path)?;
    let file_based_load_time = start.elapsed();

    println!("✓ Load time: {:?}", file_based_load_time);
    println!("✓ Memory usage: {} bytes (metadata only)", file_based.size_in_bytes());
    println!("✓ Number of triples: {}", file_based.num_triples());
    println!("✓ Indexes loaded: none (all queries via direct file I/O)");

    // 4. Indexed Streaming HDT - Configurable Approach
    println!("\n4. Indexed Streaming HDT (Configurable)");
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
    println!("\n5. Performance Comparison");
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

        // File-based
        let start = Instant::now();
        let obj_file = file_based.get_object(pos)?;
        let file_time = start.elapsed();

        // Indexed streaming
        let start = Instant::now();
        let obj_indexed = indexed_streaming.get_object(pos)?;
        let indexed_time = start.elapsed();

        println!("   Pos {}: Traditional({}) {:?}, FileBased({}) {:?}, BasicStream({}) {:?}, Indexed({}) {:?}",
                 pos, obj_traditional, traditional_time, obj_file, file_time, obj_basic, basic_time, obj_indexed, indexed_time);
    }

    println!("\n   Subject Lookup Performance:");
    for &subject_id in &test_subjects {
        // Traditional
        let start = Instant::now();
        let pos_traditional = traditional_hdt.triples.find_y(subject_id);
        let traditional_time = start.elapsed();

        // File-based
        let start = Instant::now();
        let pos_file = file_based.find_y(subject_id)?;
        let file_time = start.elapsed();

        // Basic streaming
        let start = Instant::now();
        let pos_basic = basic_streaming.find_y(subject_id)?;
        let basic_time = start.elapsed();

        // Indexed streaming
        let start = Instant::now();
        let pos_indexed = indexed_streaming.find_y(subject_id)?;
        let indexed_time = start.elapsed();

        println!("   Subject {}: Traditional({}) {:?}, FileBased({}) {:?}, BasicStream({}) {:?}, Indexed({}) {:?}",
                 subject_id, pos_traditional, traditional_time, pos_file, file_time, pos_basic, basic_time, pos_indexed, indexed_time);
    }

    // Memory usage summary
    println!("\n6. Memory Usage Summary");
    println!("----------------------");

    let traditional_mb = traditional_hdt.size_in_bytes() as f64 / (1024.0 * 1024.0);
    let file_based_bytes = file_based.size_in_bytes() as f64;
    let basic_kb = basic_streaming.size_in_bytes() as f64 / 1024.0;
    let indexed_kb = indexed_streaming.index_memory_usage() as f64 / 1024.0;

    println!("Traditional HDT:      {:.2} MB (100%)", traditional_mb);
    println!("File-Based HDT:       {} bytes ({:.4}%)", file_based_bytes, file_based_bytes / 1024.0 / 1024.0 / traditional_mb * 100.0);
    println!("Basic Streaming:      {:.2} KB ({:.1}%)", basic_kb, basic_kb / 1024.0 / traditional_mb * 100.0);
    println!("Indexed Streaming:    {:.2} KB ({:.1}%)", indexed_kb, indexed_kb / 1024.0 / traditional_mb * 100.0);

    println!("\nMemory savings vs Traditional:");
    println!("File-Based HDT:       {:.0}x less memory", traditional_mb * 1024.0 * 1024.0 / file_based_bytes.max(1.0));
    println!("Basic Streaming:      {:.1}x less memory", traditional_mb * 1024.0 / basic_kb);
    println!("Indexed Streaming:    {:.1}x less memory", traditional_mb * 1024.0 / indexed_kb.max(1.0));

    // Recommendations
    println!("\n7. Usage Recommendations");
    println!("------------------------");

    println!("🚀 Traditional HDT (TriplesBitmap):");
    println!("   ✅ Use when: Plenty of memory, frequent queries, maximum performance needed");
    println!("   ✅ Best for: Production APIs with high QPS, repeated query patterns");
    println!("   ❌ Avoid when: Memory constrained, large files, many concurrent processes");

    println!("\n💾 File-Based HDT (NEW - Zero-Index):");
    println!("   ✅ Use when: Extremely memory-constrained, files larger than RAM");
    println!("   ✅ Best for: Exploration, development, serverless, edge devices");
    println!("   ✅ Memory: Absolute minimum (~100-200 bytes metadata only)");
    println!("   ⚠️  Slow: Every access requires file I/O - combine with caching");
    println!("   ❌ Avoid when: Need high throughput or low-latency queries");

    println!("\n🔄 Basic Streaming HDT:");
    println!("   ✅ Use when: Limited memory, infrequent queries");
    println!("   ✅ Best for: Batch processing with sequential access patterns");
    println!("   ⚠️  Note: File-Based HDT is usually better choice (lower memory, similar perf)");

    println!("\n⚖️ Indexed Streaming HDT:");
    println!("   ✅ Use when: Moderate memory constraints, mixed workloads, production systems");
    println!("   ✅ Best for: Balancing memory usage with performance requirements");
    println!("   ✅ Configurable: Tune index selection based on query patterns and memory budget");
    println!("   ✅ Sweet spot: Between file-based (too slow) and traditional (too much memory)");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_implementations_consistency() -> Result<(), Box<dyn std::error::Error>> {
        // Test that all four implementations return consistent results
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let traditional = Hdt::read(BufReader::new(file))?;

        let file_based = FileBasedTripleAccess::from_file("tests/resources/snikmeta.hdt")?;

        let basic_streaming = StreamingTriplesBitmap::from_file("tests/resources/snikmeta.hdt")?;

        let indexed_streaming = StreamingIndexedTriplesBitmap::from_file_with_config(
            "tests/resources/snikmeta.hdt",
            IndexConfig::default()
        )?;

        // All should report same number of triples
        let num_triples = traditional.triples.adjlist_z.len();
        assert_eq!(num_triples, file_based.num_triples());
        assert_eq!(num_triples, basic_streaming.num_triples());
        assert_eq!(num_triples, indexed_streaming.num_triples());

        // Test object access consistency across all implementations
        for pos in 0..5.min(num_triples) {
            let obj_traditional = traditional.triples.adjlist_z.sequence.get(pos);
            let obj_file = file_based.get_object(pos)?;
            let obj_basic = basic_streaming.get_object(pos)?;
            let obj_indexed = indexed_streaming.get_object(pos)?;

            assert_eq!(obj_traditional, obj_file, "File-based mismatch at position {}", pos);
            assert_eq!(obj_traditional, obj_basic, "Basic streaming mismatch at position {}", pos);
            assert_eq!(obj_traditional, obj_indexed, "Indexed streaming mismatch at position {}", pos);
        }

        // Test subject lookup consistency
        for subject_id in 1..=3 {
            let pos_traditional = traditional.triples.find_y(subject_id);
            let pos_file = file_based.find_y(subject_id)?;
            let pos_basic = basic_streaming.find_y(subject_id)?;
            let pos_indexed = indexed_streaming.find_y(subject_id)?;

            assert_eq!(pos_traditional, pos_file, "File-based find_y mismatch for subject {}", subject_id);
            assert_eq!(pos_traditional, pos_basic, "Basic streaming find_y mismatch for subject {}", subject_id);
            assert_eq!(pos_traditional, pos_indexed, "Indexed streaming find_y mismatch for subject {}", subject_id);
        }

        Ok(())
    }

    #[test]
    fn test_memory_scaling() -> Result<(), Box<dyn std::error::Error>> {
        // Test that memory usage scales with index configuration

        // First verify file-based has minimal memory
        let file_based = FileBasedTripleAccess::from_file("tests/resources/snikmeta.hdt")?;
        assert!(file_based.size_in_bytes() < 1000,
                "File-based should use less than 1KB: {}", file_based.size_in_bytes());

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

            // File-based should always use less memory than any indexed approach
            // (unless indexed has 0 memory because no indexes fit in budget)
            if memory_usage > 0 {
                assert!(file_based.size_in_bytes() < memory_usage,
                       "File-based ({} bytes) should use less memory than indexed ({} bytes)",
                       file_based.size_in_bytes(), memory_usage);
            }

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