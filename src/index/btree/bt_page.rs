use std::sync::Arc;
use crate::error::{DbError, DbResult};
use crate::record::schema::FieldType;
use crate::storage::BlockId;
use crate::tx::Transaction;
use crate::record::{Layout, RID};
use crate::query::Constant;

/// B-tree directory and leaf pages have many commonalities:
/// in particular, their records are stored in sorted order, 
/// and pages split when full.
/// A BTPage object contains this common functionality.
pub struct BTPage<'a> {
    tx: Transaction<'a>,
    current_blk: BlockId,
    layout: Layout,
}

impl<'a> BTPage<'a> {
    pub fn new(tx: Transaction<'a>, current_blk: BlockId, layout: Layout) -> DbResult<Self> {
        tx.pin(&current_blk)?;
        Ok(BTPage {
            tx,
            current_blk,
            layout,
        })
    }

    /// Return the dataval of the record at the specified slot.
    /// @param slot the integer slot of an index record
    /// @return the dataval of the record at that slot
    pub fn get_data_val(&self, slot: usize) -> DbResult<Constant> {
        self.get_val(slot, "dataval")
    }
    
    /// Return the value of the page's flag field
    /// @return the value of the page's flag field
    pub fn get_flag(&self) -> DbResult<i32> {
        self.tx.get_int(&self.current_blk, 0)
    }
    
    /// Set the page's flag field to the specified value
    /// @param val the new value of the page flag
    pub fn set_flag(&self, val: i32) -> DbResult<()> {
        self.tx.set_int(&self.current_blk, 0, val, true)
    }

    // pub fn split(&self, split_pos: usize, flag: i32) {
    //     let new_block = self.append_new(flag);
    //     let new_page = BTPage::new(self.tx.clone(), new_block, self.layout.clone());
    // }
    
    /// Append a new block to the end of the specified B-tree file,
    /// having the specified flag value.
    /// @param flag the initial value of the flag
    /// @return a reference to the newly-created block
    pub fn append_new(&self, flag: i32) -> DbResult<BlockId> {
        let blk = self.tx.append(&self.current_blk.file_name())?;
        self.format(&blk, flag)?;
        Ok(blk)
    }

    pub fn format(&self, blk: &BlockId, flag: i32) -> DbResult<()> {
        self.tx.set_int(blk, 0, flag, false)?;
        self.tx.set_int(blk, std::mem::size_of::<i32>(), 0, false)?;  // #records = 0
        
        let rec_size = self.layout.slot_size();
        let block_size = self.tx.block_size();
        
        for pos in (2 * std::mem::size_of::<i32>()..block_size).step_by(rec_size) {
            if pos + rec_size <= block_size {
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

    // Methods called only by BTreeDir   
    /// Return the block number stored in the index record 
    /// at the specified slot.
    /// @param slot the slot of an index record
    /// @return the block number stored in that record
    pub fn get_child_num(&self, slot: usize) -> DbResult<i32> {
        self.get_int(slot, "block")
    }

    /// Insert a directory entry at the specified slot.
    /// @param slot the slot of an index record
    /// @param val the dataval to be stored
    /// @param blknum the block number to be stored
    pub fn insert_dir(&self, slot: usize, val: &Constant, blknum: i32) -> DbResult<()> {
        self.insert(slot)?;
        self.set_val(slot, "dataval", val)?;
        self.set_int(slot, "block", blknum)?;
        Ok(())
    }

    // Methods called only by BTreeLeaf
    /// Return the dataRID value stored in the specified leaf index record.
    /// @param slot the slot of the desired index record
    /// @return the dataRID value store at that slot
    pub fn get_data_rid(&self, slot: usize) -> DbResult<RID> {
        let block_num = self.get_int(slot, "block")?;
        let id = self.get_int(slot, "id")?;
        Ok(RID::new(block_num, id as usize))
    }

    /// Insert a leaf index record at the specified slot.
    /// @param slot the slot of the desired index record
    /// @param val the new dataval
    /// @param rid the new dataRID
    pub fn insert_leaf(&self, slot: usize, val: &Constant, rid: &RID) -> DbResult<()> {
        self.insert(slot)?;
        self.set_val(slot, "dataval", val)?;
        self.set_int(slot, "block", rid.block_number())?;
        self.set_int(slot, "id", rid.slot() as i32)?;
        Ok(())
    }

    /// Delete the index record at the specified slot.
    /// @param slot the slot of the deleted index record
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

    fn transfer_records(& self, slot: usize, dest: &BTPage<'_>) -> DbResult<()> {
        let mut dest_slot = 0;
        let schema = self.layout.schema();
        let records_cnt = self.records_cnt()?;
        // TODO
        while slot < records_cnt {
            dest.insert(dest_slot);
            for field_name in schema.fields() {
                dest.set_val(dest_slot, field_name, &self.get_val(slot, &field_name)?)?
            }
            self.delete(slot);
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
                Ok(Constant::integer(val))
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
    use crate::storage::FileMgr;
    use crate::log::LogMgr;
    use crate::buffer::BufferMgr;
    use tempfile::TempDir;

    #[test]
    fn test_bt_page_basic() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path().to_path_buf(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 8);
        
        let tx = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        
        let mut schema = Schema::new();
        schema.add_string_field("dataval", 20);
        schema.add_int_field("block");
        schema.add_int_field("id");
        
        let layout = Layout::new(schema);
        
        let blk = tx.append("testindex")?;
        let bt_page = BTPage::new(tx.clone(), blk.clone(), layout)?;
        
        // Test flag operations
        bt_page.set_flag(1)?;
        assert_eq!(bt_page.get_flag()?, 1);
        
        // Test record operations
        let val = Constant::string("test_value");
        let rid = RID::new(5, 10);
        
        bt_page.insert_leaf(0, &val, &rid)?;
        assert_eq!(bt_page.records_cnt()?, 1);
        
        let retrieved_rid = bt_page.get_data_rid(0)?;
        assert_eq!(retrieved_rid.block_number(), 5);
        assert_eq!(retrieved_rid.slot(), 10);
        
        let retrieved_val = bt_page.get_data_val(0)?;
        assert_eq!(retrieved_val, val);
        
        Ok(())
    }
} 