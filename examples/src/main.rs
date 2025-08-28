use simpledb::{DbResult, SimpleDB, record::Schema};

use rand::Rng;
use std::time::Instant;
use tempfile::TempDir;

const NUM_PERSONS: usize = 10000;
const NUM_SEARCHES: usize = 300;

pub struct SearchResults {
    pub total_time_ms: u128,
    pub inserts_per_second: f64,
    pub searches_per_second: f64,
    pub successful_searches: usize,
    pub failed_searches: usize,
}

pub fn run_person_search_benchmark(db: &mut SimpleDB) -> DbResult<SearchResults> {
    println!("Setting up Person table with {} records...", NUM_PERSONS);

    {
        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_int_field("age");
        schema.add_string_field("name", 20);

        let tx = db.new_tx()?;

        db.metadata_mgr().create_table("Person", &schema, tx.clone())?;
        
        // db.metadata_mgr().create_index("id_index", "Person", "id", tx.clone())?;
        tx.commit()?;
    }

    let start_insert = Instant::now();
    {
        let tx = db.new_tx()?;
        
        println!("Created Person table with schema: id, age, name");

        for i in 0..NUM_PERSONS {
            let insert_sql = format!(
                "INSERT INTO Person (id, age, name) VALUES ({}, {}, 'Person{}')",
                i + 1,
                18 + (i % 62),
                i + 1
            );

            db.planner().execute_update(&insert_sql, tx.clone()).expect(&format!("err insert at index {i}"));
        }

        tx.commit()?;
    }
    let insert_time = start_insert.elapsed();
    println!("Inserted {} persons in {} ms", NUM_PERSONS, insert_time.as_millis());

    {
        let tx = db.new_tx()?;
        println!("Performing {} random searches...", NUM_SEARCHES);
        let start_search = Instant::now();
        let mut rng = rand::rng();
        let mut successful_searches = 0;
        let mut failed_searches = 0;

        for i in 0..NUM_SEARCHES {
            let search_id = rng.random_range(1..=NUM_PERSONS);
            let expected_name = format!("Person{}", search_id);

            let select_sql = format!("SELECT name FROM Person WHERE id = {}", search_id);
            let plan = db.planner().create_query_plan(&select_sql, tx.clone())?;
            let mut scan = plan.open(tx.clone());

            let mut found = false;
            scan.before_first()?;
            while scan.next()? {
                let name = scan.get_string("name")?;
                if name == expected_name {
                    found = true;
                    break;
                }
            }
            scan.close();

            if found {
                successful_searches += 1;
            } else {
                failed_searches += 1;
                println!("Search {}: Expected to find Person{} but didn't", i + 1, search_id);
            }

            if (i + 1) % 100 == 0 {
                println!("Completed {} searches...", i + 1);
            }
        }

        let search_time = start_search.elapsed();
        let total_time = start_insert.elapsed();

        let results = SearchResults {
            total_time_ms: total_time.as_millis(),
            inserts_per_second: NUM_PERSONS as f64 / insert_time.as_secs_f64(),
            searches_per_second: NUM_SEARCHES as f64 / search_time.as_secs_f64(),
            successful_searches,
            failed_searches,
        };

        tx.commit()?;
        Ok(results)
    }
}

fn print_search_results(results: &SearchResults) {
    println!("\n=== Person Search Benchmark Results ===");
    println!("Total time (including setup): {} ms", results.total_time_ms);
    println!("Insert performance: {:.2} ops per second", results.inserts_per_second);
    println!("Search performance: {:.2} ops per second", results.searches_per_second);
    println!("Successful searches: {}", results.successful_searches);
    println!("Failed searches: {}", results.failed_searches);
}

fn main() -> DbResult<()> {
    let temp_dir = TempDir::new().unwrap();
    println!("Creating database in: {:?}", temp_dir.path());
    let mut db = SimpleDB::new(temp_dir.path())?;

    let results = run_person_search_benchmark(&mut db)?;
    print_search_results(&results);

    println!("\nBenchmark completed. Database will be cleaned up in 10 seconds...");
    std::thread::sleep(std::time::Duration::from_secs(10));

    Ok(())
}
