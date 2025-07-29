use crate::error::{DbError, DbResult};
use crate::record::schema::FieldType;
use crate::storage::BlockId;
use crate::tx::Transaction;
use crate::record::{Layout, RID};
use crate::query::Constant;

const DATAVAL_FIELD: &str = "dataval";
const BLOCK_FIELD: &str = "block";
const ID_FIELD: &str = "id";

/// B-tree directory and leaf pages have many commonalities:
/// in particular, their records are stored in sorted order, 
/// and pages split when full. A BTPage object contains this 
/// common functionality.
pub struct BTPage<'a> {
    tx: Transaction<'a>,
    current_blk: BlockId,
    layout: Layout,
}

impl<'a> BTPage<'a> {
    pub fn new(tx: Transaction<'a>, current_blk: BlockId, layout: Layout) -> DbResult<Self> {
        let schema= layout.schema();
        if !schema.has_field(DATAVAL_FIELD) {
            return Err(DbError::Schema(format!("{} must be present in the schema", DATAVAL_FIELD)));
        }
        if !schema.has_field(BLOCK_FIELD) {
            return Err(DbError::Schema(format!("{} must be present in the schema", BLOCK_FIELD)));
        }
        if !schema.has_field(ID_FIELD) {
            return Err(DbError::Schema(format!("{} must be present in the schema", ID_FIELD)));
        }

        tx.pin(&current_blk)?;
        Ok(BTPage {
            tx,
            current_blk,
            layout,
        })
    }

    /// Return the dataval of the record at the specified slot.
    pub fn get_data_val(&self, slot: usize) -> DbResult<Constant> {
        self.get_val(slot, DATAVAL_FIELD)
    }
    
    /// Return the value of the page's flag field
    pub fn get_flag(&self) -> DbResult<i32> {
        self.tx.get_int(&self.current_blk, 0)
    }
    
    /// Set the page's flag field to the specified value
    pub fn set_flag(&self, val: i32) -> DbResult<()> {
        self.tx.set_int(&self.current_blk, 0, val, true)
    }

    pub fn split(&self, split_pos: usize, flag: i32) -> DbResult<BlockId> {
        let new_block = self.append_new(flag)?;
        let new_page = BTPage::new(self.tx.clone(), new_block.clone(), self.layout.clone())?;
        self.transfer_records(split_pos, &new_page)?;
        new_page.set_flag(flag)?;
        return Ok(new_block);
    }
    
    /// Append a new block to the end of the specified B-tree file,
    /// having the specified flag value.
    pub fn append_new(&self, flag: i32) -> DbResult<BlockId> {
        let blk = self.tx.append(&self.current_blk.file_name())?;
        self.tx.pin(&blk)?;
        self.format(&blk, flag)?;
        self.tx.unpin(&blk);
        Ok(blk)
    }

    pub fn format(&self, blk: &BlockId, flag: i32) -> DbResult<()> {
        self.tx.set_int(blk, 0, flag, false)?;
        self.tx.set_int(blk, std::mem::size_of::<i32>(), 0, false)?;  // #records = 0
        
        let slot_size = self.layout.slot_size();
        let block_size = self.tx.block_size();
        
        for pos in (2 * std::mem::size_of::<i32>()..block_size).step_by(slot_size) {
            if pos + slot_size <= block_size {
                self.make_default_record(blk, pos)?;
            }
        }
        Ok(())
    }

    fn make_default_record(&self, blk: &BlockId, pos: usize) -> DbResult<()> {
        for field_name in self.layout.schema().fields() {
            let offset = self.layout.offset(field_name)
                .ok_or_else(|| DbError::FieldNotFound(field_name.clone()))?;
            
            match self.layout.schema().field_type(field_name) {
                Some(FieldType::Integer) => {
                    self.tx.set_int(blk, pos + offset, 0, false)?;
                }
                Some(FieldType::Varchar) => {
                    self.tx.set_string(blk, pos + offset, "", false)?;
                }
                None => return Err(DbError::FieldNotFound(field_name.clone())),
            }
        }
        Ok(())
    }

    /// Find the slot before the first record that is greater than or equal to the search key.
    /// Returns the slot index of the last record that is less than the search key.
    /// Returns -1 if all records are greater than or equal to the search key.
    pub fn find_slot_before(&self, search_key: &Constant) -> DbResult<i32> {
        let mut slot = 0;
        let num_recs = self.records_cnt()?;
        
        while slot < num_recs {
            let data_val = self.get_data_val(slot)?;
            if data_val.compare_to(search_key) >= std::cmp::Ordering::Equal {
                break;
            }
            slot += 1;
        }
        
        Ok(slot as i32 - 1)
    }

    /// Return the block number stored in the index record at the specified slot.
    pub fn get_child_cnt(&self, slot: usize) -> DbResult<i32> {
        self.get_int(slot, BLOCK_FIELD)
    }

    /// Insert a directory entry at the specified slot.
    pub fn insert_dir(&self, slot: usize, val: &Constant, blk_num: i32) -> DbResult<()> {
        self.insert(slot)?;
        self.set_val(slot, DATAVAL_FIELD, val)?;
        self.set_int(slot, BLOCK_FIELD, blk_num)?;
        Ok(())
    }

    /// Return the dataRID value stored in the specified leaf index record.
    pub fn get_data_rid(&self, slot: usize) -> DbResult<RID> {
        let block_num = self.get_int(slot, BLOCK_FIELD)?;
        let id = self.get_int(slot, ID_FIELD)?;
        Ok(RID::new(block_num, id as usize))
    }

    /// Insert a leaf index record at the specified slot.
    pub fn insert_leaf(&self, slot: usize, val: &Constant, rid: &RID) -> DbResult<()> {
        self.insert(slot)?;
        self.set_val(slot, DATAVAL_FIELD, val)?;
        self.set_int(slot, BLOCK_FIELD, rid.block_number())?;
        self.set_int(slot, ID_FIELD, rid.slot() as i32)?;
        Ok(())
    }

    /// Delete the index record at the specified slot.
    pub fn delete(&self, slot: usize) -> DbResult<()> {
        let records_cnt = self.records_cnt()?;
        for i in (slot + 1)..records_cnt {
            self.copy_record(i, i - 1)?;
        }
        self.set_records_cnt(records_cnt - 1)?;
        Ok(())
    }

    /// Return the number of index records in this page.
    /// @return the number of index records in this page
    pub fn records_cnt(&self) -> DbResult<usize> {
        let cnt = self.tx.get_int(&self.current_blk, std::mem::size_of::<i32>())?;
        Ok(cnt as usize)
    }

    /// Check if the page is full (adding one more record would exceed block size).
    /// @return true if the page is full, false otherwise
    pub fn is_full(&self) -> DbResult<bool> {
        let current_records = self.records_cnt()?;
        let next_slot_pos = self.slot_pos(current_records + 1);
        let block_size = self.tx.block_size();
        
        Ok(next_slot_pos >= block_size)
    }

    // TODO this is very inefficient, should just memcpy recs
    fn transfer_records(&self, slot: usize, dest: &BTPage<'_>) -> DbResult<()> {
        let mut dest_slot = 0;
        let schema = self.layout.schema();
        // TODO
        while slot < self.records_cnt()? {
            dest.insert(dest_slot)?;
            for field_name in schema.fields() {
                dest.set_val(dest_slot, field_name, &self.get_val(slot, &field_name)?)?
            }
            self.delete(slot)?;
            dest_slot += 1;
        }
        Ok(())
    }

    fn get_int(&self, slot: usize, fldname: &str) -> DbResult<i32> {
        let pos = self.fld_pos(slot, fldname)?;
        self.tx.get_int(&self.current_blk, pos)
    }

    fn get_string(&self, slot: usize, fldname: &str) -> DbResult<String> {
        let pos = self.fld_pos(slot, fldname)?;
        self.tx.get_string(&self.current_blk, pos)
    }

    fn get_val(&self, slot: usize, fldname: &str) -> DbResult<Constant> {
        let field_type = self.layout.schema().field_type(fldname)
            .ok_or_else(|| DbError::FieldNotFound(fldname.to_string()))?;

        match field_type {
            FieldType::Integer => {
                let val = self.get_int(slot, fldname)?;
                Ok(Constant::int(val))
            }
            FieldType::Varchar => {
                let val = self.get_string(slot, fldname)?;
                Ok(Constant::string(val))
            }
        }
    }

    fn set_int(&self, slot: usize, fldname: &str, val: i32) -> DbResult<()> {
        let pos = self.fld_pos(slot, fldname)?;
        self.tx.set_int(&self.current_blk, pos, val, true)
    }
    
    fn set_string(&self, slot: usize, fldname: &str, val: &str) -> DbResult<()> {
        let pos = self.fld_pos(slot, fldname)?;
        self.tx.set_string(&self.current_blk, pos, val, true)
    }

    fn set_val(&self, slot: usize, fldname: &str, val: &Constant) -> DbResult<()> {
        let field_type = self.layout.schema().field_type(fldname)
            .ok_or_else(|| DbError::FieldNotFound(fldname.to_string()))?;
            
        match field_type {
            FieldType::Integer => {
                self.set_int(slot, fldname, val.as_integer())
            }
            FieldType::Varchar => {
                self.set_string(slot, fldname, val.as_string())
            }
        }
    }

    fn set_records_cnt(&self, n: usize) -> DbResult<()> {
        self.tx.set_int(&self.current_blk, std::mem::size_of::<i32>(), n as i32, true)
    }

    fn insert(&self, slot: usize) -> DbResult<()> {
        let records_cnt = self.records_cnt()?;
        for i in (slot..records_cnt).rev() {
            self.copy_record(i, i + 1)?;
        }
        self.set_records_cnt(records_cnt + 1)?;
        Ok(())
    }

    fn copy_record(&self, from: usize, to: usize) -> DbResult<()> {
        let schema = self.layout.schema();
        for field_name in schema.fields() {
            let val = self.get_val(from, field_name)?;
            self.set_val(to, field_name, &val)?;
        }
        Ok(())
    }

    fn fld_pos(&self, slot: usize, fldname: &str) -> DbResult<usize> {
        let offset = self.layout.offset(fldname)
            .ok_or_else(|| DbError::FieldNotFound(fldname.to_string()))?;
        Ok(self.slot_pos(slot) + offset)
    }

    fn slot_pos(&self, slot: usize) -> usize {
        let slot_size = self.layout.slot_size();
        std::mem::size_of::<i32>() + std::mem::size_of::<i32>() + (slot * slot_size)
    }
}

impl<'a> Drop for BTPage<'a> {
    fn drop(&mut self) {
        self.tx.unpin(&self.current_blk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Schema;
    use crate::utils::testing_utils::temp_db;

    struct TestRecord {
        id: i32,
        block: i32,
        dataval: &'static str,
    }

    impl TestRecord {
        fn new(id: i32, block: i32, dataval: &'static str) -> Self {
            TestRecord { id, block, dataval }
        }

        fn insert_into(&self, page: &BTPage<'_>, slot: usize) -> DbResult<()> {
            page.insert(slot)?;
            page.set_int(slot, ID_FIELD, self.id)?;
            page.set_int(slot, BLOCK_FIELD, self.block)?;
            page.set_string(slot, DATAVAL_FIELD, self.dataval)?;
            Ok(())
        }

        fn assert_in_page(&self, page: &BTPage<'_>, slot: usize) -> DbResult<()> {
            assert_eq!(self.id, page.get_int(slot, ID_FIELD)?);
            assert_eq!(self.block, page.get_int(slot, BLOCK_FIELD)?);
            assert_eq!(self.dataval, page.get_string(slot, DATAVAL_FIELD)?);
            Ok(())
        }
    }

    #[test]
    fn test_btree_page_record_iter() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field(DATAVAL_FIELD, 5);
        schema.add_int_field(BLOCK_FIELD);
        schema.add_int_field(ID_FIELD);

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        let page = BTPage::new(tx.clone(), blk.clone(), layout)?;

        assert_eq!(0, page.records_cnt()?);

        let records = vec![
            TestRecord::new(1, 42, "ABCDE"),
            TestRecord::new(2, 99, "ZXCVB"),
            TestRecord::new(3, 115, "QWERT"),
        ];

        for (slot, record) in records.iter().enumerate() {
            record.insert_into(&page, slot)?;
        }

        assert_eq!(3, page.records_cnt()?);

        for (slot, record) in records.iter().enumerate() {
            record.assert_in_page(&page, slot)?;
        }

        Ok(())
    }

    #[test]
    fn test_page_split() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field(DATAVAL_FIELD, 5);
        schema.add_int_field(BLOCK_FIELD);
        schema.add_int_field(ID_FIELD);

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        let page = BTPage::new(tx.clone(), blk.clone(), layout.clone())?;
        
        // Insert records into the original page
        let records = vec![
            TestRecord::new(1, 42, "ABCDE"),
            TestRecord::new(2, 99, "ZXCVB"),
            TestRecord::new(3, 115, "QWERT"),
            TestRecord::new(4, 200, "ZGSVA"),
        ];

        for (slot, record) in records.iter().enumerate() {
            record.insert_into(&page, slot)?;
        }

        assert_eq!(4, page.records_cnt()?);

        let new_block = page.split(2, 777)?;
        let new_page = BTPage::new(tx.clone(), new_block.clone(), layout.clone())?;

        assert_eq!(2, page.records_cnt()?);
        records[0].assert_in_page(&page, 0)?;
        records[1].assert_in_page(&page, 1)?;

        assert_eq!(2, new_page.records_cnt()?);
        records[2].assert_in_page(&new_page, 0)?;
        records[3].assert_in_page(&new_page, 1)?;

        assert_eq!(777, new_page.get_flag()?);
        Ok(())
    }

    #[test]
    fn test_bt_page_basic() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field(DATAVAL_FIELD, 10);
        schema.add_int_field(BLOCK_FIELD);
        schema.add_int_field(ID_FIELD);

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        let page = BTPage::new(tx.clone(), blk.clone(), layout)?;
        
        // Test flag operations
        page.set_flag(1)?;
        assert_eq!(page.get_flag()?, 1);
        
        // Test record operations
        let val: Constant = Constant::string("test_value");
        let rid = RID::new(5, 10);
        
        page.insert_leaf(0, &val, &rid)?;
        assert_eq!(page.records_cnt()?, 1);
        
        let retrieved_rid = page.get_data_rid(0)?;
        assert_eq!(retrieved_rid.block_number(), 5);
        assert_eq!(retrieved_rid.slot(), 10);
        
        let retrieved_val = page.get_data_val(0)?;
        assert_eq!(retrieved_val, val);
        
        Ok(())
    }

    #[test]
    fn test_find_slot_before_string() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field(DATAVAL_FIELD, 5);
        schema.add_int_field(BLOCK_FIELD);
        schema.add_int_field(ID_FIELD);

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        let page = BTPage::new(tx.clone(), blk.clone(), layout)?;

        let records = vec![
            TestRecord::new(1, 42, "AAAAA"),
            TestRecord::new(2, 99, "BBBBB"),
            TestRecord::new(3, 115, "CCCCC"),
            TestRecord::new(4, 200, "DDDDD"),
        ];

        for (slot, record) in records.iter().enumerate() {
            record.insert_into(&page, slot)?;
        }

        assert_eq!(page.find_slot_before(&Constant::string("AAAAA"))?, -1); // Before first
        assert_eq!(page.find_slot_before(&Constant::string("BBBBB"))?, 0);  // After first
        assert_eq!(page.find_slot_before(&Constant::string("CCCCC"))?, 1);  // After second
        assert_eq!(page.find_slot_before(&Constant::string("DDDDD"))?, 2);  // After third
        assert_eq!(page.find_slot_before(&Constant::string("EEEEE"))?, 3);  // After last

        Ok(())
    }

    #[test]
    fn test_find_slot_before_int() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_int_field(DATAVAL_FIELD);
        schema.add_int_field(BLOCK_FIELD);
        schema.add_int_field(ID_FIELD);

        let layout = Layout::new(schema);
        let blk = tx.append("testindex_int")?;
        let page = BTPage::new(tx.clone(), blk.clone(), layout)?;

        for i in 0..5i32 {
            let slot = i as usize;
            page.insert(slot)?;
            page.set_int(slot, DATAVAL_FIELD, i * 10)?;
            page.set_int(slot, BLOCK_FIELD, i)?;
            page.set_int(slot, ID_FIELD, i)?;
        }

        assert_eq!(page.find_slot_before(&Constant::int(0))?, -1);
        assert_eq!(page.find_slot_before(&Constant::int(10))?, 0);
        assert_eq!(page.find_slot_before(&Constant::int(20))?, 1);
        assert_eq!(page.find_slot_before(&Constant::int(30))?, 2);
        assert_eq!(page.find_slot_before(&Constant::int(40))?, 3);
        assert_eq!(page.find_slot_before(&Constant::int(50))?, 4);

        Ok(())
    }
} 