use crate::storage::block_id::BlockId;
use crate::tx::transaction::Transaction;
use crate::error::{DbError, DbResult};
use crate::query::{Constant, Scan, UpdateScan};
use super::layout::Layout;
use super::record_page::RecordPage;
use super::row_id::RowId;
use super::schema::{FieldType, Schema};

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

    fn find_first_slot(&mut self) -> DbResult<bool> {
        if let Some(rp) = &self.record_page {
            if let Some(slot) = rp.next_after(0)? {
                self.current_slot = Some(slot);
                return Ok(true);
            }
        }
        Ok(false)
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
        let mut record_page = RecordPage::new(self.tx.clone(), blk, self.layout.clone())?;
        record_page.format()?;
        self.record_page = Some(record_page);
        self.current_slot = None;
        Ok(())
    }

    pub fn close(&mut self) {
        self.record_page.take();
    }
}

impl<'a> Scan for TableScan<'a> {
    fn before_first(&mut self) -> DbResult<()> {
        self.move_to_block(0)
    }

    fn next(&mut self) -> DbResult<bool> {
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

    fn get_int(&mut self, field_name: &str) -> DbResult<i32> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.get_int(slot, field_name)
    }
    
    fn get_string(&mut self, field_name: &str) -> DbResult<String> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.get_string(slot, field_name)
    }
    
    fn get_val(&mut self, field_name: &str) -> DbResult<Constant> {
        match self.layout.schema().field_type(field_name) {
            Some(FieldType::Integer) => {
                let val = self.get_int(field_name)?;
                Ok(Constant::Integer(val))
            },
            Some(FieldType::Varchar) => {
                let val = self.get_string(field_name)?;
                Ok(Constant::String(val))
            },
            None => Err(DbError::FieldNotFound(field_name.to_string())),
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }
    
    fn close(&mut self) {
        self.record_page.take();
    }
    
    fn schema(&self) -> &Schema {
        self.layout.schema()
    }
}

impl<'a> UpdateScan for TableScan<'a> {
    fn set_val(&mut self, field_name: &str, val: Constant) -> DbResult<()> {
        match val {
            Constant::Integer(i) => self.set_int(field_name, i),
            Constant::String(s) => self.set_string(field_name, &s),
        }
    }

    fn set_int(&mut self, field_name: &str, val: i32) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.set_int(slot, field_name, val)
    }
    
    fn set_string(&mut self, field_name: &str, val: &str) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.set_string(slot, field_name, val)
    }

    fn insert(&mut self) -> DbResult<()> {
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

    fn delete(&mut self) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self.record_page.as_ref().expect("Record page not initialized");
        rp.delete(slot)
    }

    fn get_rid(&self) -> DbResult<RowId> {
        let slot = self.current_slot.expect("No current record");
        let record_page = self.record_page.as_ref().expect("Record page not initialized");
        Ok(RowId::new(record_page.block().number(), slot))
    }

    fn move_to_rid(&mut self, row_id: RowId) -> DbResult<()> {
        self.record_page.take();

        let blk = BlockId::new(self.file_name.clone(), row_id.block_number());
        self.record_page = Some(RecordPage::new(self.tx.clone(), blk, self.layout.clone())?);
        self.current_slot = Some(row_id.slot() as usize);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::{buffer::buffer_mgr::BufferMgr, log::LogMgr, query::{Scan, UpdateScan}, record::schema::Schema, storage::file_mgr::FileMgr};

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
        table_scan.set_string("name", "Alice")?;
        
        table_scan.insert()?;
        table_scan.set_int("id", 2)?;
        table_scan.set_string("name", "Bob")?;
        
        table_scan.insert()?;
        table_scan.set_int("id", 3)?;
        table_scan.set_string("name", "Charlie")?;

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
            
            let id_val = table_scan.get_val("id")?;
            assert!(id_val.is_integer());
            assert_eq!(id_val.as_integer(), id);
            
            let name_val = table_scan.get_val("name")?;
            assert!(name_val.is_string());
            assert_eq!(name_val.as_string(), name);
        }

        assert_eq!(count, 3, "Should have read 3 records");

        table_scan.before_first()?;
        table_scan.next()?;
        table_scan.delete()?;

        table_scan.before_first()?;
        count = 0;
        while table_scan.next()? {
            count += 1;
        }
        assert_eq!(count, 2, "Should have 2 records after deletion");
        
        table_scan.before_first()?;
        table_scan.next()?;
        let rid = table_scan.get_rid()?;
        let id1 = table_scan.get_int("id")?;
        
        table_scan.next()?;
        let id2 = table_scan.get_int("id")?;
        
        table_scan.move_to_rid(rid)?;
        let id_check = table_scan.get_int("id")?;
        assert_eq!(id1, id_check, "RID navigation failed");
        
        table_scan.close();
        tx.commit()?;

        Ok(())
    }
}