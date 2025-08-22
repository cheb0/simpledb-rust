use simpledb::{DbResult, SimpleDB, storage::BlockId, tx::Transaction};

use rand::Rng;
use std::time::Instant;
use tempfile::TempDir;

const FILE_NAME: &str = "benchmark.dat";
const NUM_BLOCKS: usize = 100;
const NUM_TRANSACTIONS: usize = 50000;
const OPERATIONS_PER_TX: usize = 3;
const INT_FIELDS_PER_BLOCK: usize = 5;
const STRING_FIELDS_PER_BLOCK: usize = 5;
const STRING_LENGTH: usize = 10;
const NUM_ITERATIONS: usize = 3;

pub struct BenchmarkResults {
    pub total_time_ms: u128,
    pub transactions_per_second: f64,
    pub operations_per_second: f64,
}

pub fn run_benchmark_iteration(db: &mut SimpleDB) -> DbResult<BenchmarkResults> {
    prepare_database(db)?;

    let start = Instant::now();

    for _ in 0..NUM_TRANSACTIONS {
        execute_random_operations_in_tx(db)?;
    }

    let duration = start.elapsed();
    let total_operations = NUM_TRANSACTIONS * OPERATIONS_PER_TX;

    Ok(BenchmarkResults {
        total_time_ms: duration.as_millis(),
        transactions_per_second: NUM_TRANSACTIONS as f64 / duration.as_secs_f64(),
        operations_per_second: total_operations as f64 / duration.as_secs_f64(),
    })
}

pub fn run_benchmark(db: &mut SimpleDB, iterations: usize) -> DbResult<BenchmarkResults> {
    let mut total_time_ms = 0;
    let mut total_tx_per_sec = 0.0;
    let mut total_ops_per_sec = 0.0;

    println!("Running {} benchmark iterations...", iterations);

    for i in 0..iterations {
        println!("Iteration {} of {}", i + 1, iterations);
        let results = run_benchmark_iteration(db)?;
        print_iteration_results(&results, i + 1);

        total_time_ms += results.total_time_ms;
        total_tx_per_sec += results.transactions_per_second;
        total_ops_per_sec += results.operations_per_second;
    }

    // Calculate averages
    let avg_results = BenchmarkResults {
        total_time_ms: total_time_ms / iterations as u128,
        transactions_per_second: total_tx_per_sec / iterations as f64,
        operations_per_second: total_ops_per_sec / iterations as f64,
    };

    Ok(avg_results)
}

fn prepare_database(db: &mut SimpleDB) -> DbResult<()> {
    let tx: Transaction<'_> = db.new_tx()?;

    for i in 0..NUM_BLOCKS {
        let blk = tx.append(FILE_NAME)?;
        tx.pin(&blk)?;

        for j in 0..INT_FIELDS_PER_BLOCK {
            let offset = j * 4; // 4 bytes per int
            tx.set_int(&blk, offset, (i * j) as i32, true)?;
        }

        let string_offset = INT_FIELDS_PER_BLOCK * 4;
        for j in 0..STRING_FIELDS_PER_BLOCK {
            let offset = string_offset + j * (STRING_LENGTH + 4); // string length + length prefix
            tx.set_string(&blk, offset, &format!("init-{}-{}", i, j), true)?;
        }

        tx.unpin(&blk);
    }

    tx.commit()?;
    Ok(())
}

fn execute_random_operations_in_tx(db: &mut SimpleDB) -> DbResult<()> {
    let mut rng = rand::rng();
    let tx = db.new_tx()?;

    for _ in 0..OPERATIONS_PER_TX {
        let block_num = rng.random_range(0..NUM_BLOCKS) as i32;
        let blk = BlockId::new(FILE_NAME.to_string(), block_num);

        tx.pin(&blk)?;

        if rng.random_bool(0.5) {
            let field_num = rng.random_range(0..INT_FIELDS_PER_BLOCK);
            let offset = field_num * 4;
            let value = rng.random::<i32>();
            tx.set_int(&blk, offset, value, true)?;
        } else {
            let field_num = rng.random_range(0..STRING_FIELDS_PER_BLOCK);
            let string_offset = INT_FIELDS_PER_BLOCK * 4;
            let offset = string_offset + field_num * (STRING_LENGTH + 4);

            let value: String = (0..rng.random_range(5..STRING_LENGTH))
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect();

            tx.set_string(&blk, offset, &value, true)?;
        }

        tx.unpin(&blk);
    }
    tx.commit()?;

    Ok(())
}

fn print_iteration_results(results: &BenchmarkResults, iteration: usize) {
    println!("=== Iteration {} Results ===", iteration);
    println!("Total time: {} ms", results.total_time_ms);
    println!(
        "Transactions per second: {:.2}",
        results.transactions_per_second
    );
    println!(
        "Operations per second: {:.2}",
        results.operations_per_second
    );
    println!();
}

pub fn print_benchmark_results(results: &BenchmarkResults) {
    println!("=== Average Benchmark Results ===");
    println!("Average time per iteration: {} ms", results.total_time_ms);
    println!(
        "Average transactions per second: {:.2}",
        results.transactions_per_second
    );
    println!(
        "Average operations per second: {:.2}",
        results.operations_per_second
    );
}

fn main() -> DbResult<()> {
    let temp_dir = TempDir::new().unwrap();
    println!("Temp dir:{:?}", temp_dir);
    let mut db = SimpleDB::new(temp_dir.path())?;

    let results = run_benchmark(&mut db, NUM_ITERATIONS)?;
    print_benchmark_results(&results);

    std::thread::sleep(std::time::Duration::from_secs(30));

    Ok(())
}
