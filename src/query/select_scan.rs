use crate::error::DbResult;
use crate::query::{Scan, Predicate, Constant};

/// A scan that filters records based on a predicate.
pub struct SelectScan<'a> {
    scan: Box<dyn Scan + 'a>,
    pred: Predicate,
}

impl<'a> SelectScan<'a> {
    pub fn new(s: Box<dyn Scan + 'a>, pred: Predicate) -> Self {
        SelectScan { scan: s, pred }
    }
}

impl<'a> Scan for SelectScan<'a> {
    fn before_first(&mut self) -> DbResult<()> {
        self.scan.before_first()
    }

    fn next(&mut self) -> DbResult<bool> {
        while self.scan.next()? {
            if self.pred.is_satisfied(&mut *self.scan)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_int(&mut self, field_name: &str) -> DbResult<i32> {
        self.scan.get_int(field_name)
    }

    fn get_string(&mut self, field_name: &str) -> DbResult<String> {
        self.scan.get_string(field_name)
    }

    fn get_val(&mut self, field_name: &str) -> DbResult<Constant> {
        self.scan.get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.scan.has_field(field_name)
    }

    fn close(&mut self) {
        self.scan.close();
    }
}

/* impl<'a> UpdateScan for SelectScan<'a> {
    fn set_val(&mut self, field_name: &str, val: Constant) -> DbResult<()> {
        self.scan.set_val(field_name, val)
    }

    fn set_int(&mut self, field_name: &str, val: i32) -> DbResult<()> {
        self.scan.set_int(field_name, val)
    }

    fn set_string(&mut self, field_name: &str, val: &str) -> DbResult<()> {
        self.scan.set_string(field_name, val)
    }

    fn insert(&mut self) -> DbResult<()> {
        self.scan.insert()
    }

    fn delete(&mut self) -> DbResult<()> {
        self.scan.delete()
    }

    fn get_rid(&self) -> DbResult<RID> {
        self.scan.get_rid()
    }

    fn move_to_rid(&mut self, rid: RID) -> DbResult<()> {
        self.scan.move_to_rid(rid)
    }
} */

#[cfg(test)]
mod tests {
    use crate::{
        query::{Constant, Expr, Predicate, Scan, Term, UpdateScan},
        record::{schema::Schema, table_scan::TableScan},
        utils::testing_utils::temp_db,
    };

    use super::*;

    #[test]
    fn test_select_scan() -> DbResult<()> {
        let db = temp_db()?;
        
        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        let layout = crate::record::layout::Layout::new(schema);
        let tx = db.new_tx()?;

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
            Expr::field_name("id"),
            Expr::constant(Constant::Int(1))
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