#[cfg(test)]
mod tests {
    use simpledb::{SimpleDB, DbResult};
    use tempfile::TempDir;

    #[test]
    fn test_insert_and_select() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        println!("Test database directory: {:?}", temp_dir.path());

        let db = SimpleDB::new(temp_dir.path())?;
        let planner = db.planner();

        {
            let tx = db.new_tx()?;
            db.planner().execute_update("CREATE TABLE test_table(id int, name VARCHAR(10), age int)", tx.clone())?;
            tx.commit()?;
        }
        
        {
            let tx = db.new_tx()?;
            planner.execute_update("INSERT INTO test_table(id, name, age) VALUES(1, 'John', 25)", tx.clone())?;
            planner.execute_update("INSERT INTO test_table(id, name, age) VALUES(2, 'Jack', 21)", tx.clone())?;
            planner.execute_update("INSERT INTO test_table(id, name, age) VALUES(3, 'Alice', 22)", tx.clone())?;
            planner.execute_update("INSERT INTO test_table(id, name, age) VALUES(4, 'Bob', 24)", tx.clone())?;
            planner.execute_update("INSERT INTO test_table(id, name, age) VALUES(5, 'Chad', 26)", tx.clone())?;
            
            tx.commit()?;
        }

        {
            let tx: simpledb::tx::Transaction<'_> = db.new_tx()?;
            let plan = db.planner().create_query_plan("SELECT id, name FROM test_table WHERE id = 3", tx.clone())?;
            let mut scan = plan.open(tx.clone());

            scan.before_first()?;
            
            assert!(scan.next()?, "Should find a record with id = 3");
            
            let id = scan.get_int("id")?;
            let name = scan.get_string("name")?;
            
            assert_eq!(id, 3, "ID should be 3");
            assert_eq!(name, "Alice", "Name should be 'Alice'");
            assert!(!scan.next()?, "Should not find any more records");
            
            tx.commit()?;
        }

        {
            let tx = db.new_tx()?;
            let plan = db.planner().create_query_plan("SELECT id, name, age FROM test_table", tx.clone())?;
            let mut scan = plan.open(tx.clone());

            scan.before_first()?;
            
            let mut records = Vec::new();
            while scan.next()? {
                let id = scan.get_int("id")?;
                let name = scan.get_string("name")?;
                let age = scan.get_int("age")?;
                records.push((id, name, age));
            }
            
            assert_eq!(records.len(), 5, "Should have exactly 5 records");
            
            let expected_records = vec![
                (1, "John".to_string(), 25),
                (2, "Jack".to_string(), 21),
                (3, "Alice".to_string(), 22),
                (4, "Bob".to_string(), 24),
                (5, "Chad".to_string(), 26),
            ];
            
            assert_eq!(records, expected_records, "All records should match expected values");
            
            tx.commit()?;
        }

        Ok(())
    }

    #[test]
    fn test_update_statement() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        println!("Test database directory: {:?}", temp_dir.path());

        let db = SimpleDB::new(temp_dir.path())?;
        let planner = db.planner();

        {
            let tx = db.new_tx()?;
            planner.execute_update("CREATE TABLE users(id int, name VARCHAR(10), age int)", tx.clone())?;
            tx.commit()?;
        }
        
        {
            let tx = db.new_tx()?;
            planner.execute_update("INSERT INTO users(id, name, age) VALUES(1, 'Alice', 25)", tx.clone())?;
            planner.execute_update("INSERT INTO users(id, name, age) VALUES(2, 'Bob', 30)", tx.clone())?;
            planner.execute_update("INSERT INTO users(id, name, age) VALUES(3, 'Charlie', 35)", tx.clone())?;
            tx.commit()?;
        }

        {
            let tx = db.new_tx()?;
            let affected_rows = planner.execute_update("UPDATE users SET age = 26 WHERE id = 1", tx.clone())?;
            assert_eq!(affected_rows, 1, "Should update exactly 1 row");
            tx.commit()?;
        }

        {
            let tx = db.new_tx()?;
            let plan = planner.create_query_plan("SELECT id, name, age FROM users WHERE id = 1", tx.clone())?;
            let mut scan = plan.open(tx.clone());

            scan.before_first()?;
            assert!(scan.next()?, "Should find the updated record");
            
            let id = scan.get_int("id")?;
            let name = scan.get_string("name")?;
            let age = scan.get_int("age")?;
            
            assert_eq!(id, 1, "ID should be 1");
            assert_eq!(name, "Alice", "Name should still be 'Alice'");
            assert_eq!(age, 26, "Age should be updated to 26");
            
            tx.commit()?;
        }

        {
            let tx = db.new_tx()?;
            let affected_rows = planner.execute_update("UPDATE users SET age = 40", tx.clone())?;
            assert_eq!(affected_rows, 3, "Should update all 3 rows");
            tx.commit()?;
        }

        {
            let tx = db.new_tx()?;
            let plan = planner.create_query_plan("SELECT id, name, age FROM users", tx.clone())?;
            let mut scan = plan.open(tx.clone());

            scan.before_first()?;
            
            let mut records = Vec::new();
            while scan.next()? {
                let id = scan.get_int("id")?;
                let name = scan.get_string("name")?;
                let age = scan.get_int("age")?;
                records.push((id, name, age));
            }
            
            assert_eq!(records.len(), 3, "Should have exactly 3 records");
            
            for (_id, _name, age) in &records {
                assert_eq!(*age, 40, "All ages should be 40 after bulk update");
            }
            
            tx.commit()?;
        }

        Ok(())
    }
}
