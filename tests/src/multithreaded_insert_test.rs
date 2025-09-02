#[cfg(test)]
mod tests {
    use simpledb::server::config::StorageMgrConfig;
    use simpledb::server::Config;
    use simpledb::{DbResult, SimpleDB};
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_multithreaded_inserts_and_count() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cfg = Config::new(StorageMgrConfig::file(temp_dir.path()));
        let db = Arc::new(SimpleDB::with_config(cfg)?);
        let planner = db.planner();

        {
            let tx = db.new_tx()?;
            planner.execute_update(
                "CREATE TABLE persons(id int, name VARCHAR(20), age int)",
                tx.clone(),
            )?;
            tx.commit()?;
        }

        const NUM_THREADS: usize = 4;
        const INSERTS_PER_THREAD: usize = 1000;

        let mut handles = Vec::new();
        for thread_id in 0..NUM_THREADS {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || -> DbResult<usize> {
                let start_id = thread_id * INSERTS_PER_THREAD;
                let end_id = start_id + INSERTS_PER_THREAD;
                
                let mut inserts = 0;
                for id in start_id..end_id {
                    let tx = db_clone.new_write_tx()?;
                    let sql = format!(
                        "INSERT INTO persons(id, name, age) VALUES({}, 'Person{}', {})",
                        id + 1,
                        id + 1,
                        18 + (id % 62)
                    );
                    db_clone.planner().execute_update(&sql, tx.clone())?;
                    tx.commit()?;
                    inserts += 1;
                }
                Ok(inserts)
            });
            handles.push(handle);
        }

        let mut total_inserted = 0;
        for handle in handles {
            total_inserted += handle.join().unwrap()?;
        }

        {
            let tx = db.new_tx()?;
            let plan = db
                .planner()
                .create_query_plan("SELECT id, name, age FROM persons", tx.clone())?;
            let mut scan = plan.open(tx.clone());
            scan.before_first()?;

            let mut count = 0;
            while scan.next()? {
                count += 1;
            }

            assert_eq!(count, total_inserted, "Row count must equal total inserts");
            tx.commit()?;
        }

        Ok(())
    }
}