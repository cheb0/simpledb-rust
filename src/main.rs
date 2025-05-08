use simpledb::{SimpleDB, DbResult};

fn main() -> DbResult<()> {
    let db = SimpleDB::new("path/to/db")?;
    Ok(())
}