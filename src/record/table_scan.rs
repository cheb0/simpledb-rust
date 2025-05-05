use crate::storage::block_id::BlockId;
use crate::tx::transaction::Transaction;
use crate::error::{DbError, DbResult};
use super::layout::Layout;
use super::record_page::RecordPage;
use super::row_id::RowId;

pub struct TableScan<'a> {
    tx: Transaction<'a>,
    layout: Layout,
    record_page: Option<RecordPage<'a>>,
    file_name: String,
    current_slot: Option<usize>,
}

impl<'a> TableScan<'a> {
    pub fn new(tx: Transaction<'a>, table_name: &str, layout: Layout) -> DbResult<Self> {
        let file_name = format!("{}.tbl", table_name);
        let mut table_scan = TableScan {
            tx,
            layout,
            record_page: None,
            file_name,
            current_slot: None,
        };
    
        if table_scan.tx.size(&table_scan.file_name)? == 0 {
            table_scan.move_to_new_block()?;
        } else {
            table_scan.move_to_block(0)?;
        }

        Ok(table_scan)
    }

    pub fn before_first(&mut self) -> DbResult<()> {
        self.move_to_block(0)
    }

    pub fn next(&mut self) -> DbResult<bool> {
        let current = self.current_slot.unwrap_or(0);
        
        if let Some(rp) = &self.record_page {
            if let Some(slot) = rp.next_after(current)? {
                self.current_slot = Some(slot);
                return Ok(true);
            }
            
            if !self.at_last_block()? {
                let next_block = rp.block().number() + 1;
                self.move_to_block(next_block)?;
                return self.find_first_slot();
            }
        }
        
        Ok(false)
    }

    fn find_first_slot(&mut self) -> DbResult<bool> {
        if let Some(rp) = &self.record_page {
            if let Some(slot) = rp.next_after(0)? {
                self.current_slot = Some(slot);
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn get_int(&self, field_name: &str) -> DbResult<i32> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.get_int(slot, field_name)
    }
    
    pub fn get_string(&self, field_name: &str) -> DbResult<String> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.get_string(slot, field_name)
    }

    pub fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }

    pub fn set_int(&self, field_name: &str, val: i32) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.set_int(slot, field_name, val)
    }
    
    pub fn set_string(&self, field_name: &str, val: String) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.set_string(slot, field_name, val)
    }

    pub fn get_rid(&self) -> DbResult<RowId> {
        let slot = self.current_slot.expect("No current record");
        let rp: &RecordPage<'a> = self.record_page.as_ref().expect("Record page not initialized");
        Ok(RowId::new(rp.block().number(), slot))
    }

    pub fn delete(&self) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.delete(slot)
    }

    pub fn move_to_rid(&mut self, rid: RowId) -> DbResult<()> {
        self.record_page.take();

        let blk = BlockId::new(self.file_name.clone(), rid.block_number());
        self.record_page = Some(RecordPage::new(self.tx.clone(), blk, self.layout.clone())?);
        self.current_slot = Some(rid.slot());
        Ok(())
    }

    fn at_last_block(&self) -> DbResult<bool> {
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        let size = self.tx.size(&self.file_name)?;
        Ok(rp.block().number() == size - 1)
    }

    fn move_to_block(&mut self, blk_number: i32) -> DbResult<()> {
        self.record_page.take();

        let blk = BlockId::new(self.file_name.clone(), blk_number);
        self.record_page = Some(RecordPage::new(self.tx.clone(), blk, self.layout.clone())?);
        self.current_slot = None;
        Ok(())
    }

    fn move_to_new_block(&mut self) -> DbResult<()> {
        self.record_page.take();

        let blk = self.tx.append(&self.file_name)?;
        let mut rp = RecordPage::new(self.tx.clone(), blk, self.layout.clone())?;
        rp.format()?;
        self.record_page = Some(rp);
        self.current_slot = None;
        Ok(())
    }

    pub fn insert(&mut self) -> DbResult<()> {
        let current = self.current_slot.unwrap_or(0);
        
        if let Some(rp) = &self.record_page {
            if let Some(slot) = rp.insert_after(current)? {
                self.current_slot = Some(slot);
                return Ok(());
            }
            
            let at_last = self.at_last_block()?;
            
            if at_last {
                self.move_to_new_block()?;
            } else {
                let next_block = rp.block().number() + 1;
                self.move_to_block(next_block)?;
            }
        } else {
            self.move_to_new_block()?;
        }
        
        if let Some(rp) = &self.record_page {
            if let Some(slot) = rp.insert_after(0)? {
                self.current_slot = Some(slot);
                return Ok(());
            }
        }
        
        Err(DbError::NoAvailableSlot)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::{buffer::buffer_mgr::BufferMgr, log::LogMgr, record::schema::Schema, storage::file_mgr::FileMgr};

    use super::*;

    #[test]
    fn test() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        let layout = Layout::new(schema);
        let mut tx = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;

        let blk1 = tx.append("testfile");
        let blk2 = tx.append("testfile");
        let blk3 = tx.append("testfile");

        let mut table_scan = TableScan::new(tx.clone(), "test_table", layout)?;

        table_scan.insert()?;
        table_scan.set_int("id", 1)?;
        table_scan.set_string("name", "Alice".to_string())?;
        
        table_scan.insert()?;
        table_scan.set_int("id", 2)?;
        table_scan.set_string("name", "Bob".to_string())?;
        
        table_scan.insert()?;
        table_scan.set_int("id", 3)?;
        table_scan.set_string("name", "Charlie".to_string())?;

        table_scan.before_first()?;

        let mut count = 0;
        while table_scan.next()? {
            count += 1;
            let id = table_scan.get_int("id")?;
            let name = table_scan.get_string("name")?;
            
            match id {
                1 => assert_eq!(name, "Alice"),
                2 => assert_eq!(name, "Bob"),
                3 => assert_eq!(name, "Charlie"),
                _ => panic!("Unexpected ID: {}", id),
            }
        }

        assert_eq!(count, 3, "Should have read 3 records");

        // Test delete
        table_scan.before_first()?;
        table_scan.next()?;  // Move to first record
        table_scan.delete()?;

        // Verify deletion
        table_scan.before_first()?;
        count = 0;
        while table_scan.next()? {
            count += 1;
        }
        assert_eq!(count, 2, "Should have 2 records after deletion");
        
        tx.commit()?;

        Ok(())
    }
}