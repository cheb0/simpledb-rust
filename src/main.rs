use simpledb::{DbResult, SimpleDB};
use std::sync::Arc;
use std::thread;
use std::time::{Instant, Duration};
use tempfile::TempDir;

const NUM_WRITER_THREADS: usize = 4;
const ROWS_PER_WRITER: usize = 10000;
const INITIAL_ROWS: usize = 5;

fn main() -> DbResult<()> {
    let temp_dir = TempDir::new().unwrap();
    println!("Test database directory: {:?}", temp_dir.path());
    
    let db = Arc::new(SimpleDB::new(temp_dir.path())?);
    let planner = db.planner();

    {
        let tx = db.new_tx()?;
        planner.execute_update(
            "create table test_table(id int, name VARCHAR(20), age int)",
            tx.clone(),
        )?;
        tx.commit()?;
    }

/*     {
        let tx = db.new_tx()?;
        db.metadata_mgr()
            .create_index("id_index", "test_table", "id", tx.clone())?;
        println!("Created index on id field");
        tx.commit()?;
    } */

    {
        let tx = db.new_write_tx()?;
        println!("Inserting {} initial rows...", INITIAL_ROWS);

        for i in 1..=INITIAL_ROWS {
            let name = match i {
                1 => "John",
                2 => "Jack",
                3 => "Alice",
                4 => "Bob",
                5 => "Chad",
                _ => "Unknown"
            };
            let age = 20 + i;

            db.planner().execute_update(
                &format!("insert into test_table(id, name, age) values({}, '{}', {})", i, name, age),
                tx.clone(),
            )?;
        }
        tx.commit()?;
        println!("Initial rows inserted successfully");
    }

    println!("\nStarting {} writer threads, each inserting {} rows...", NUM_WRITER_THREADS, ROWS_PER_WRITER);

    let start_time = Instant::now();

    let mut handles = Vec::new();
    for thread_id in 0..NUM_WRITER_THREADS {
        let db_clone = Arc::clone(&db);

        let handle = thread::spawn(move || {
            let mut successful_inserts = 0;

            let start_id = INITIAL_ROWS + 1 + (thread_id * ROWS_PER_WRITER);
            let end_id = start_id + ROWS_PER_WRITER - 1;

            for row_num in 0..ROWS_PER_WRITER {
                let id = start_id + row_num;
                let name = format!("Thread{}_Row{}", thread_id, row_num + 1);
                let age = 18 + (id % 62);
                let insert_sql = format!(
                    "insert into test_table(id, name, age) values({}, '{}', {})",
                    id, name, age
                );

                let tx = db_clone.new_write_tx().unwrap();
                db_clone.planner().execute_update(&insert_sql, tx.clone()).unwrap();
                tx.commit().unwrap();
                successful_inserts += 1;
                thread::sleep(Duration::from_millis(1));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_time = start_time.elapsed();
    let total_inserts = NUM_WRITER_THREADS * ROWS_PER_WRITER;
    println!("\nAll writer threads completed in {:.2} seconds", total_time.as_secs_f64());
    println!("Total inserts: {} rows", total_inserts);
    println!("Insert performance: {:.2} rows per second",
        total_inserts as f64 / total_time.as_secs_f64());

    {
        let tx = db.new_tx()?;
        let plan = db
            .planner()
            .create_query_plan("SELECT id, name, age FROM test_table", tx.clone())?;
        let mut scan = plan.open(tx.clone());

        let mut total_rows = 0;
        scan.before_first()?;
        while scan.next()? {
            total_rows += 1;
        }
        scan.close();

        println!("\nFinal table contains {} total rows", total_rows);
        println!("Expected: {} initial + {} inserted = {} total",
            INITIAL_ROWS, total_inserts, INITIAL_ROWS + total_inserts);
        tx.commit()?;
    }

    println!("\nConcurrent insert test completed successfully!");
    println!("Database will be cleaned up in 5 seconds...");
    thread::sleep(Duration::from_secs(5));

    Ok(())
}
