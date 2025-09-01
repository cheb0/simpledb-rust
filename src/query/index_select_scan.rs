use crate::error::DbResult;
use crate::index::Index;
use crate::query::{Constant, Scan, UpdateScan};
use crate::record::TableScan;

/// `IndexSelectScan` uses an index to efficiently find records matching a specific value.
/// It combines an index scan with a table scan to retrieve the actual record data.
pub struct IndexSelectScan<'tx> {
    table_scan: TableScan<'tx>,
    index: Box<dyn Index + 'tx>,
    search_value: Constant,
}

impl<'tx> IndexSelectScan<'tx> {
    pub fn new(
        table_scan: TableScan<'tx>,
        index: Box<dyn Index + 'tx>,
        search_value: Constant,
    ) -> DbResult<Self> {
        let mut scan = IndexSelectScan {
            table_scan,
            index,
            search_value,
        };
        scan.before_first()?;
        Ok(scan)
    }
}

impl<'tx> Scan for IndexSelectScan<'tx> {
    /// Positions the scan before the first record matching the search value.
    /// This positions the index before the first instance of the selection constant.
    fn before_first(&mut self) -> DbResult<()> {
        self.index.before_first(&self.search_value)
    }

    /// Moves to the next record matching the search value.
    /// If there is a next record, the method moves the table scan to the corresponding data record.
    /// Returns false if there are no more matching records.
    fn next(&mut self) -> DbResult<bool> {
        let has_next = self.index.next()?;
        if has_next {
            let rid = self.index.get_data_rid()?;
            self.table_scan.move_to_rid(rid)?;
        }
        Ok(has_next)
    }

    fn get_int(&mut self, field_name: &str) -> DbResult<i32> {
        self.table_scan.get_int(field_name)
    }

    fn get_string(&mut self, field_name: &str) -> DbResult<String> {
        self.table_scan.get_string(field_name)
    }

    fn get_val(&mut self, field_name: &str) -> DbResult<Constant> {
        self.table_scan.get_val(field_name)
    }

    /// Returns whether the data record has the specified field.
    fn has_field(&self, field_name: &str) -> bool {
        self.table_scan.has_field(field_name)
    }
}

impl<'tx> Drop for IndexSelectScan<'tx> {
    fn drop(&mut self) {
        self.index.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::BTreeIndex,
        metadata::IndexInfo,
        record::{layout::Layout, schema::Schema},
        utils::testing_utils::temp_db,
    };

    #[test]
    fn test_index_select_scan_basic() -> DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");
        let layout = Layout::new(schema);

        let tx = db.new_tx()?;

        let index_layout = IndexInfo::create_idx_layout("age", layout.schema());
        let mut index = BTreeIndex::new(tx.clone(), "age_idx", index_layout)?;

        let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
        scan.insert()?;
        scan.set_int("id", 1)?;
        scan.set_string("name", "Alice")?;
        scan.set_int("age", 25)?;
        let rid1 = scan.get_rid()?;

        scan.insert()?;
        scan.set_int("id", 2)?;
        scan.set_string("name", "Bob")?;
        scan.set_int("age", 30)?;
        let rid2 = scan.get_rid()?;

        scan.insert()?;
        scan.set_int("id", 3)?;
        scan.set_string("name", "Charlie")?;
        scan.set_int("age", 25)?;
        let rid3 = scan.get_rid()?;

        // Insert into index
        index.insert(&Constant::int(25), &rid1)?;
        index.insert(&Constant::int(30), &rid2)?;
        index.insert(&Constant::int(25), &rid3)?;

        // Create index select scan for age = 25
        let mut index_scan = IndexSelectScan::new(
            TableScan::new(tx.clone(), "test_table", layout.clone())?,
            Box::new(index),
            Constant::int(25),
        )?;

        // Should find 2 records with age = 25
        let mut count = 0;
        let mut ages = Vec::new();
        let mut names = Vec::new();

        while index_scan.next()? {
            count += 1;
            let age = index_scan.get_int("age")?;
            let name = index_scan.get_string("name")?;
            ages.push(age);
            names.push(name);
        }

        assert_eq!(count, 2, "Should find 2 records with age = 25");
        assert_eq!(ages, vec![25, 25]);
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Charlie".to_string()));

        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_index_select_scan_string_field() -> DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");
        let layout = Layout::new(schema);

        let tx = db.new_tx()?;

        let index_layout = IndexInfo::create_idx_layout("name", layout.schema());
        let mut index = BTreeIndex::new(tx.clone(), "name_idx", index_layout)?;

        let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
        scan.insert()?;
        scan.set_int("id", 1)?;
        scan.set_string("name", "Alice")?;
        scan.set_int("age", 25)?;
        let rid1 = scan.get_rid()?;

        scan.insert()?;
        scan.set_int("id", 2)?;
        scan.set_string("name", "Bob")?;
        scan.set_int("age", 30)?;
        let rid2 = scan.get_rid()?;

        scan.insert()?;
        scan.set_int("id", 3)?;
        scan.set_string("name", "Alice")?;
        scan.set_int("age", 35)?;
        let rid3 = scan.get_rid()?;

        index.insert(&Constant::string("Alice"), &rid1)?;
        index.insert(&Constant::string("Bob"), &rid2)?;
        index.insert(&Constant::string("Alice"), &rid3)?;

        let mut count = 0;
        let mut names = Vec::new();
        let mut ages = Vec::new();

        {
            let mut index_scan = IndexSelectScan::new(
                TableScan::new(tx.clone(), "test_table", layout.clone())?,
                Box::new(index),
                Constant::string("Alice"),
            )?;

            while index_scan.next()? {
                count += 1;
                let name = index_scan.get_string("name")?;
                let age = index_scan.get_int("age")?;
                names.push(name);
                ages.push(age);
            }
        }

        assert_eq!(count, 2, "Should find 2 records with name = 'Alice'");
        assert_eq!(names, vec!["Alice", "Alice"]);
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_index_select_scan_no_matches() -> DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");
        let layout = Layout::new(schema);

        let tx = db.new_tx()?;

        let index_layout = IndexInfo::create_idx_layout("age", layout.schema());
        let mut index = BTreeIndex::new(tx.clone(), "age_idx", index_layout)?;

        let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
        scan.insert()?;
        scan.set_int("id", 1)?;
        scan.set_string("name", "Alice")?;
        scan.set_int("age", 25)?;
        let rid = scan.get_rid()?;

        index.insert(&Constant::int(25), &rid)?;

        {
            let mut index_scan = IndexSelectScan::new(
                TableScan::new(tx.clone(), "test_table", layout.clone())?,
                Box::new(index),
                Constant::int(99),
            )?;
    
            let mut count = 0;
            while index_scan.next()? {
                count += 1;
            }
    
            assert_eq!(count, 0, "Should find no records with age = 99");
        }

        tx.commit()?;
        Ok(())
    }
}
