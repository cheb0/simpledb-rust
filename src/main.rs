use simpledb::{plan::{project_plan::ProjectPlan, table_plan::TablePlan, Plan}, query::UpdateScan, record::{Layout, Schema, TableScan}, DbResult, SimpleDB};
use tempfile::TempDir;

fn main() -> DbResult<()> {
    let temp_dir = TempDir::new().unwrap();
    println!("Temp dir:{:?}", temp_dir);
    
    let db = SimpleDB::new(temp_dir)?;
    
    let mut schema = Schema::new();
    schema.add_int_field("id");
    schema.add_string_field("name", 20);

    let layout = Layout::new(schema.clone());

    {
        let tx = db.new_tx()?;
    
        db.metadata_mgr().create_table("test_table", &schema, tx.clone())?;
    
        {
            let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
    
            scan.insert()?;
            scan.set_int("id", 1)?;
            scan.set_string("name", "Alice")?;
            
            scan.insert()?;
            scan.set_int("id", 2)?;
            scan.set_string("name", "Bob")?;
            
            scan.insert()?;
            scan.set_int("id", 3)?;
            scan.set_string("name", "Charlie")?;
        }
        tx.commit()?;
    }

    {
        let tx = db.new_tx()?;
        let table_plan = TablePlan::new(tx.clone(), "test_table", &db.metadata_mgr())?;
        let project_plan = ProjectPlan::new(Box::new(table_plan), vec!["name".to_string()]);
        
        let mut scan = project_plan.open();

        scan.before_first()?;

        while scan.next()? {
            // let id = scan.get_int("id")?;
            let name = scan.get_string("name")?;
            println!("name: {}", name);
        }

        tx.commit()?;
    }

    Ok(())
}