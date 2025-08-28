use super::RecordPage;
use super::layout::Layout;
use super::rid::RID;
use super::schema::FieldType;
use crate::error::{DbError, DbResult};
use crate::query::{Constant, Scan, UpdateScan};
use crate::storage::BlockId;
use crate::tx::Transaction;

pub struct TableScan<'tx> {
    tx: Transaction<'tx>,
    layout: Layout,
    record_page: Option<RecordPage<'tx>>,
    file_name: String,
    current_slot: Option<usize>,
}

impl<'tx> TableScan<'tx> {
    pub fn new(tx: Transaction<'tx>, table_name: &str, layout: Layout) -> DbResult<Self> {
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

    pub fn move_to_last(&mut self) -> DbResult<()> {
        let size = self.tx.size(&self.file_name)?;
        self.move_to_block(size - 1)
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
        let rp = self
            .record_page
            .as_ref()
            .expect("Record page not initialized");
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
        let record_page = RecordPage::new(self.tx.clone(), blk, self.layout.clone())?;
        record_page.format()?;
        self.record_page = Some(record_page);
        self.current_slot = None;
        Ok(())
    }

    pub fn close(&mut self) {
        self.record_page.take();
    }
}

impl<'tx> Scan for TableScan<'tx> {
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
        let rp = self
            .record_page
            .as_ref()
            .expect("Record page not initialized");
        rp.get_int(slot, field_name)
    }

    fn get_string(&mut self, field_name: &str) -> DbResult<String> {
        let slot = self.current_slot.expect("No current record");
        let rp = self
            .record_page
            .as_ref()
            .expect("Record page not initialized");
        rp.get_string(slot, field_name)
    }

    fn get_val(&mut self, field_name: &str) -> DbResult<Constant> {
        match self.layout.schema().field_type(field_name) {
            Some(FieldType::Integer) => {
                let val = self.get_int(field_name)?;
                Ok(Constant::Int(val))
            }
            Some(FieldType::Varchar) => {
                let val = self.get_string(field_name)?;
                Ok(Constant::String(val))
            }
            None => Err(DbError::FieldNotFound(field_name.to_string())),
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }

    fn close(&mut self) {
        self.record_page.take();
    }
}

impl<'tx> UpdateScan for TableScan<'tx> {
    fn set_val(&mut self, field_name: &str, val: Constant) -> DbResult<()> {
        match val {
            Constant::Int(i) => self.set_int(field_name, i),
            Constant::String(s) => self.set_string(field_name, &s),
        }
    }

    fn set_int(&mut self, field_name: &str, val: i32) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self
            .record_page
            .as_ref()
            .expect("Record page not initialized");
        rp.set_int(slot, field_name, val)
    }

    fn set_string(&mut self, field_name: &str, val: &str) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self
            .record_page
            .as_ref()
            .expect("Record page not initialized");
        rp.set_string(slot, field_name, val)
    }

    fn insert(&mut self) -> DbResult<()> {
        let current_slot = self.current_slot.unwrap_or(0);
        let current_page = self.record_page.as_ref().unwrap();
        let mut inserted_at_slot = current_page.insert_after(current_slot)?;

        loop {
            if !inserted_at_slot.is_none() {
                self.current_slot = inserted_at_slot;
                return Ok(());
            }

            // have not found place in the current page, move to the next
            // TODO just single method -> move_next()
            if self.at_last_block()? {
                self.move_to_new_block()?;
            } else {
                self.move_to_block(self.record_page.as_ref().unwrap().block().number() + 1)?;
            }

            let current_slot = self.current_slot.unwrap_or(0);
            inserted_at_slot = self.record_page.as_ref().unwrap().insert_after(current_slot)?;
        }
    }

    fn delete(&mut self) -> DbResult<()> {
        let slot = self.current_slot.expect("No current record");
        let rp = self
            .record_page
            .as_ref()
            .expect("Record page not initialized");
        rp.delete(slot)
    }

    fn get_rid(&self) -> DbResult<RID> {
        let slot = self.current_slot.expect("No current record");
        let record_page = self
            .record_page
            .as_ref()
            .expect("Record page not initialized");
        Ok(RID::new(record_page.block().number(), slot))
    }

    fn move_to_rid(&mut self, row_id: RID) -> DbResult<()> {
        self.record_page.take();

        let blk = BlockId::new(self.file_name.clone(), row_id.block_number());
        self.record_page = Some(RecordPage::new(self.tx.clone(), blk, self.layout.clone())?);
        self.current_slot = Some(row_id.slot() as usize);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        query::UpdateScan,
        record::schema::Schema,
        utils::testing_utils::{temp_db},
    };

    use super::*;


    #[test]
    fn test_insert_and_scan_in_tx() -> DbResult<()> {
        let num_keys = 500;
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_int_field("age");
        schema.add_string_field("name", 20);
        let layout = Layout::new(schema);
        
        let tx = db.new_tx()?;
                
        for i in 0..num_keys {
            let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
            scan.insert().expect(&format!("Failed to insert at {i}"));
            scan.set_int("id", i)?;
            scan.set_string("name", &format!("Entry{i}"))?;
            scan.close();
        }

        let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
        scan.before_first()?;
        let mut i = 0;
        while scan.next()? {
            assert_eq!(i, scan.get_int("id")?);
            i += 1;
        }
        scan.close();
        tx.commit()?;
        assert_eq!(num_keys, i);

        Ok(())
    }

    #[test]
    fn test_insert_and_scan_in_dedicated_tx() -> DbResult<()> {
        let num_keys = 500;
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_int_field("age");
        schema.add_string_field("name", 20);
        let layout = Layout::new(schema);
        
        {
            for i in 0..num_keys {
                let tx = db.new_tx()?;
                let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
                scan.insert().expect(&format!("Failed to insert at {i}"));
                scan.set_int("id", i)?;
                scan.set_string("name", &format!("Entry{i}"))?;
                scan.close();
                tx.commit()?;
            }
        }

        {
            let tx = db.new_tx()?;
            let mut scan = TableScan::new(tx.clone(), "test_table", layout.clone())?;
            scan.before_first()?;
            let mut i = 0;
            while scan.next()? {
                assert_eq!(i, scan.get_int("id")?);
                i += 1;
            }
            scan.close();
            tx.commit()?;
            assert_eq!(num_keys, i);
        }
        Ok(())
    }
}
