use simpledb::{DbResult, SimpleDB, record::Schema};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Instant, Duration};
use tempfile::TempDir;
use rand::Rng;

const NUM_PERSONS: usize = 10_000;
const NUM_READERS: usize = 8;
const NUM_WRITERS: usize = 2;
const READ_OPERATIONS: usize = 1000;
const WRITE_OPERATIONS: usize = 100000;
const STRING_LENGTH: usize = 20;

pub struct StressTestResults {
    pub setup_time_ms: u128,
    pub total_read_time_ms: u128,
    pub total_write_time_ms: u128,
    pub reads_per_second: f64,
    pub writes_per_second: f64,
    pub successful_reads: usize,
    pub failed_reads: usize,
    pub successful_writes: usize,
    pub failed_writes: usize,
}

pub fn run_stress_test(db_ptr: Arc<SimpleDB>) -> DbResult<StressTestResults> {
    println!("Setting up Person table with {} records...", NUM_PERSONS);
    
    let mut schema = Schema::new();
    schema.add_int_field("id");
    schema.add_int_field("age");
    schema.add_string_field("name", STRING_LENGTH);
    
    let setup_start = Instant::now();
    {
        let tx = db_ptr.new_tx()?;
        db_ptr.metadata_mgr().create_table("Person", &schema, tx.clone())?;
        println!("Created Person table with schema: id, age, name(20)");
        tx.commit()?;
    }
    
    println!("Populating table with {} records...", NUM_PERSONS);
    {
        let tx = db_ptr.new_tx()?;
        for i in 0..NUM_PERSONS {
            let insert_sql = format!(
                "INSERT INTO Person (id, age, name) VALUES ({}, {}, 'Person{}')",
                i + 1,
                18 + (i % 62), // Age range: 18-79
                i + 1
            );
            db_ptr.planner().execute_update(&insert_sql, tx.clone())?;
            
            if (i + 1) % 500 == 0 {
                println!("Inserted {} records...", i + 1);
            }
        }
        tx.commit()?;
    }
    
    let setup_time = setup_start.elapsed();
    println!("Setup completed in {} ms", setup_time.as_millis());
    
    let results = Arc::new(Mutex::new(StressTestResults {
        setup_time_ms: setup_time.as_millis(),
        total_read_time_ms: 0,
        total_write_time_ms: 0,
        reads_per_second: 0.0,
        writes_per_second: 0.0,
        successful_reads: 0,
        failed_reads: 0,
        successful_writes: 0,
        failed_writes: 0,
    }));
    
    println!("Starting {} reader threads...", NUM_READERS);
    
    println!("Starting {} writer threads...", NUM_WRITERS);
    let mut writer_handles = vec![];
    for writer_id in 0..NUM_WRITERS {
        let db = Arc::clone(&db_ptr);
        
        let results_clone = results.clone();
        
        let handle = thread::spawn(move || {
            let start_time = Instant::now();
            let mut successful = 0;
            let mut failed = 0;
            
            for op in 0..WRITE_OPERATIONS {
                let new_id = NUM_PERSONS + 1 + op + (writer_id * WRITE_OPERATIONS);
                let new_age = 18 + (new_id % 62);
                let new_name = format!("Person{}", new_id);
                let insert_sql = format!(
                    "INSERT INTO Person (id, age, name) VALUES ({}, {}, '{}')",
                    new_id, new_age, new_name
                );

                let tx = db.new_write_tx().unwrap();
                
                match db.planner().execute_update(&insert_sql, tx.clone()) {
                    Ok(_) => {
                        successful += 1;
                    }
                    Err(_) => {
                        failed += 1;
                    }
                }
                tx.commit().unwrap();
                
                if (op + 1) % 200 == 0 {
                    println!("Writer {}: Completed {} operations", writer_id, op + 1);
                }
                
                // Small delay to simulate real-world scenario
                thread::sleep(Duration::from_millis(1));
            }
            
            let duration = start_time.elapsed();

            let mut results_guard = results_clone.lock().unwrap();
            results_guard.total_write_time_ms += duration.as_millis();
            results_guard.successful_writes += successful;
            results_guard.failed_writes += failed;
            
            println!("Writer {} completed: {} successful, {} failed in {} ms", 
                writer_id, successful, failed, duration.as_millis());
        });
        writer_handles.push(handle);
    }

    println!("Waiting for all threads to complete...");
/*     for handle in reader_handles {
        handle.join().unwrap();
    } */
    for handle in writer_handles {
        handle.join().unwrap();
    }
    
    let mut results_guard = results.lock().unwrap();
    let total_read_ops = NUM_READERS * READ_OPERATIONS;
    let total_write_ops = NUM_WRITERS * WRITE_OPERATIONS;
    
    results_guard.reads_per_second = total_read_ops as f64 / (results_guard.total_read_time_ms as f64 / 1000.0);
    results_guard.writes_per_second = total_write_ops as f64 / (results_guard.total_write_time_ms as f64 / 1000.0);
    
    let final_results = StressTestResults {
        setup_time_ms: results_guard.setup_time_ms,
        total_read_time_ms: results_guard.total_read_time_ms,
        total_write_time_ms: results_guard.total_write_time_ms,
        reads_per_second: results_guard.reads_per_second,
        writes_per_second: results_guard.writes_per_second,
        successful_reads: results_guard.successful_reads,
        failed_reads: results_guard.failed_reads,
        successful_writes: results_guard.successful_writes,
        failed_writes: results_guard.failed_writes,
    };
    
    Ok(final_results)
}

fn print_stress_results(results: &StressTestResults) {
    println!("\n=== Stress Test Results ===");
    println!("Setup time: {} ms", results.setup_time_ms);
    println!("Total read time: {} ms", results.total_read_time_ms);
    println!("Total write time: {} ms", results.total_write_time_ms);
    println!("Read performance: {:.2} ops per second", results.reads_per_second);
    println!("Write performance: {:.2} ops per second", results.writes_per_second);
    println!("Successful reads: {}", results.successful_reads);
    println!("Failed reads: {}", results.failed_reads);
    println!("Successful writes: {}", results.successful_writes);
    println!("Failed writes: {}", results.failed_writes);
    println!("Read success rate: {:.1}%", 
        (results.successful_reads as f64 / (results.successful_reads + results.failed_reads) as f64) * 100.0);
    println!("Write success rate: {:.1}%", 
        (results.successful_writes as f64 / (results.successful_writes + results.failed_writes) as f64) * 100.0);
}

fn main() -> DbResult<()> {
    let temp_dir = TempDir::new().unwrap();
    println!("Creating database in: {:?}", temp_dir.path());
    println!("Configuration:");
    println!("  - {} reader threads, {} operations each", NUM_READERS, READ_OPERATIONS);
    println!("  - {} writer threads, {} operations each", NUM_WRITERS, WRITE_OPERATIONS);
    println!("  - Total: {} read operations, {} write operations", 
        NUM_READERS * READ_OPERATIONS, NUM_WRITERS * WRITE_OPERATIONS);
    
    let db = Arc::new(SimpleDB::new(temp_dir.path())?);
    
    let results = run_stress_test(Arc::clone(&db))?;
    print_stress_results(&results);
    
    println!("\nStress test completed. Database will be cleaned up in 10 seconds...");
    thread::sleep(Duration::from_secs(10));
    
    Ok(())
} 