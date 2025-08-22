use simpledb::{DbResult, SimpleDB};
use tempfile::TempDir;

fn main() -> DbResult<()> {
    let temp_dir = TempDir::new().unwrap();
    println!("Test database directory: {:?}", temp_dir.path());

    let db = SimpleDB::new(temp_dir.path())?;
    let planner = db.planner();

    {
        let tx = db.new_tx()?;
        let upd = planner.execute_update(
            "create table test_table(id int, name VARCHAR(20), age int)",
            tx.clone(),
        )?;
        println!("Create table: {:?}", upd);
        tx.commit()?;
    }

    {
        let tx = db.new_tx()?;
        db.planner().execute_update(
            "insert into test_table(id, name, age) values(1, 'John', 25)",
            tx.clone(),
        )?;
        db.planner().execute_update(
            "insert into test_table(id, name, age) values(2, 'Jack', 21)",
            tx.clone(),
        )?;
        db.planner().execute_update(
            "insert into test_table(id, name, age) values(3, 'Alice', 22)",
            tx.clone(),
        )?;
        db.planner().execute_update(
            "insert into test_table(id, name, age) values(4, 'Bob', 24)",
            tx.clone(),
        )?;
        db.planner().execute_update(
            "insert into test_table(id, name, age) values(5, 'Chad', 26)",
            tx.clone(),
        )?;
        tx.commit()?;
    }

    {
        let tx = db.new_tx()?;
        let plan = db
            .planner()
            .create_query_plan("SELECT id, name FROM test_table WHERE id = 3", tx.clone())?;
        let mut scan = plan.open(tx.clone());

        scan.before_first()?;
        while scan.next()? {
            let id = scan.get_int("id")?;
            let name = scan.get_string("name")?;
            println!("id={:} with name={:}", id, name);
        }

        tx.commit()?;
    }

    Ok(())
}
