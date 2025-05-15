use crate::error::DbResult;
use crate::query::{Scan, UpdateScan, Predicate, Constant};
use crate::record::{Schema, RowId};

/// A scan that filters records based on a predicate.
pub struct SelectScan<'a> {
    s: Box<dyn UpdateScan + 'a>,
    pred: Predicate,
}

impl<'a> SelectScan<'a> {
    /// Creates a new select scan that filters records from the underlying scan
    /// based on the given predicate.
    pub fn new(s: Box<dyn UpdateScan + 'a>, pred: Predicate) -> Self {
        SelectScan { s, pred }
    }
}

impl<'a> Scan for SelectScan<'a> {
    fn before_first(&mut self) -> DbResult<()> {
        self.s.before_first()
    }

    fn next(&mut self) -> DbResult<bool> {
        while self.s.next()? {
            if self.pred.is_satisfied(&mut *self.s)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_int(&mut self, field_name: &str) -> DbResult<i32> {
        self.s.get_int(field_name)
    }

    fn get_string(&mut self, field_name: &str) -> DbResult<String> {
        self.s.get_string(field_name)
    }

    fn get_val(&mut self, field_name: &str) -> DbResult<Constant> {
        self.s.get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.s.has_field(field_name)
    }

    fn close(&mut self) {
        self.s.close();
    }

    fn schema(&self) -> &Schema {
        self.s.schema()
    }
}

impl<'a> UpdateScan for SelectScan<'a> {
    fn set_val(&mut self, field_name: &str, val: Constant) -> DbResult<()> {
        self.s.set_val(field_name, val)
    }

    fn set_int(&mut self, field_name: &str, val: i32) -> DbResult<()> {
        self.s.set_int(field_name, val)
    }

    fn set_string(&mut self, field_name: &str, val: &str) -> DbResult<()> {
        self.s.set_string(field_name, val)
    }

    fn insert(&mut self) -> DbResult<()> {
        self.s.insert()
    }

    fn delete(&mut self) -> DbResult<()> {
        self.s.delete()
    }

    fn get_rid(&self) -> DbResult<RowId> {
        self.s.get_rid()
    }

    fn move_to_rid(&mut self, rid: RowId) -> DbResult<()> {
        self.s.move_to_rid(rid)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::{
        buffer::BufferMgr,
        log::LogMgr,
        query::{Constant, Expression, Predicate, Scan, Term, UpdateScan},
        record::{schema::Schema, table_scan::TableScan},
        storage::file_mgr::FileMgr,
        tx::transaction::Transaction,
    };

    use super::*;

    #[test]
    fn test_select_scan() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        let layout = crate::record::layout::Layout::new(schema);
        let tx = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;

        let mut table_scan = TableScan::new(tx.clone(), "test_table", layout)?;
        
        table_scan.insert()?;
        table_scan.set_int("id", 1)?;
        table_scan.set_string("name", "Alice")?;
        
        table_scan.insert()?;
        table_scan.set_int("id", 2)?;
        table_scan.set_string("name", "Bob")?;
        
        table_scan.insert()?;
        table_scan.set_int("id", 3)?;
        table_scan.set_string("name", "Charlie")?;

        // Create predicate: id = 1
        let pred = Predicate::new(Term::new(
            Expression::with_field_name("id"),
            Expression::with_constant(Constant::Integer(1))
        ));

        let mut select_scan = SelectScan::new(Box::new(table_scan), pred);

        select_scan.before_first()?;
        
        // Should only get records with id = 1
        let mut count = 0;
        while select_scan.next()? {
            count += 1;
            let id = select_scan.get_int("id")?;
            let name = select_scan.get_string("name")?;
            
            assert_eq!(id, 1, "Filtered record should have id > 1");
            
            match id {
                1 => assert_eq!(name, "Alice"),
                _ => panic!("Unexpected ID: {}", id),
            }
        }
        assert_eq!(count, 1, "Should have found one record with id = 1");
        
        select_scan.close();
        tx.commit()?;

        Ok(())
    }
}