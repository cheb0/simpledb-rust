use super::Layout;
use super::schema::FieldType;
use crate::error::DbResult;
use crate::storage::BlockId;
use crate::tx::Transaction;

const EMPTY: i32 = 0;
const USED: i32 = 1;

pub struct RecordPage<'a> {
    tx: Transaction<'a>,
    blk: BlockId,
    layout: Layout,
}

impl<'a> RecordPage<'a> {
    pub fn new(tx: Transaction<'a>, blk: BlockId, layout: Layout) -> DbResult<Self> {
        tx.pin(&blk)?;
        Ok(RecordPage { tx, blk, layout })
    }

    pub fn get_int(&self, slot: usize, field_name: &str) -> DbResult<i32> {
        let field_pos =
            self.offset(slot) + self.layout.offset(field_name).expect("Field not found");
        self.tx.get_int(&self.blk, field_pos)
    }

    pub fn get_string(&self, slot: usize, field_name: &str) -> DbResult<String> {
        let field_pos =
            self.offset(slot) + self.layout.offset(field_name).expect("Field not found");
        self.tx.get_string(&self.blk, field_pos)
    }

    pub fn set_int(&self, slot: usize, field_name: &str, val: i32) -> DbResult<()> {
        let field_pos =
            self.offset(slot) + self.layout.offset(field_name).expect("Field not found");
        self.tx.set_int(&self.blk, field_pos, val, true)
    }

    pub fn set_string(&self, slot: usize, field_name: &str, val: &str) -> DbResult<()> {
        let field_pos =
            self.offset(slot) + self.layout.offset(field_name).expect("Field not found");
        self.tx.set_string(&self.blk, field_pos, val, true)
    }

    pub fn delete(&self, slot: usize) -> DbResult<()> {
        self.set_flag(slot, EMPTY)
    }

    pub fn format(&self) -> DbResult<()> {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            self.tx
                .set_int(&self.blk, self.offset(slot), EMPTY, false)?;

            for field_name in self.layout.schema().fields() {
                let field_pos =
                    self.offset(slot) + self.layout.offset(field_name).expect("Field not found");

                match self
                    .layout
                    .schema()
                    .field_type(field_name)
                    .expect("Field type not found")
                {
                    FieldType::Integer => {
                        self.tx.set_int(&self.blk, field_pos, 0, false)?;
                    }
                    FieldType::Varchar => {
                        self.tx.set_string(&self.blk, field_pos, "", false)?;
                    }
                }
            }
            slot += 1;
        }
        Ok(())
    }

    pub fn next_after(&self, slot: usize) -> DbResult<Option<usize>> {
        self.search_after(slot, USED)
    }

    pub fn insert_after(&self, slot: usize) -> DbResult<Option<usize>> {
        if let Some(new_slot) = self.search_after(slot, EMPTY)? {
            self.set_flag(new_slot, USED)?;
            Ok(Some(new_slot))
        } else {
            Ok(None)
        }
    }

    pub fn block(&self) -> &BlockId {
        &self.blk
    }

    fn set_flag(&self, slot: usize, flag: i32) -> DbResult<()> {
        self.tx.set_int(&self.blk, self.offset(slot), flag, true)
    }

    fn search_after(&self, mut slot: usize, flag: i32) -> DbResult<Option<usize>> {
        slot += 1;
        while self.is_valid_slot(slot) {
            if self.tx.get_int(&self.blk, self.offset(slot))? == flag {
                return Ok(Some(slot));
            }
            slot += 1;
        }
        Ok(None)
    }

    fn is_valid_slot(&self, slot: usize) -> bool {
        self.offset(slot + 1) <= self.tx.block_size()
    }

    fn offset(&self, slot: usize) -> usize {
        slot * self.layout.slot_size()
    }
}

impl<'a> Drop for RecordPage<'a> {
    fn drop(&mut self) {
        self.tx.unpin(&self.blk);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DbResult;
    use crate::record::schema::Schema;
    use crate::utils::testing_utils::temp_db_with_cfg;

    #[test]
    fn test_record_page_basic() -> DbResult<()> {
        let db = temp_db_with_cfg(|cfg| cfg.buffer_capacity(3))?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        let layout = Layout::new(schema);
        let tx = db.new_tx()?;

        let blk = tx.append("testfile")?;

        let buffer_mgr = db.buffer_mgr();
        assert_eq!(3, buffer_mgr.available());

        {
            let record_page = RecordPage::new(tx.clone(), blk.clone(), layout)?;

            assert_eq!(2, buffer_mgr.available()); // one buffer is pinned by record_page

            record_page.format()?;
            let slot = record_page.insert_after(0)?.expect("Failed to insert");
            record_page.set_int(slot, "id", 123)?;
            record_page.set_string(slot, "name", "test")?;
            assert_eq!(record_page.get_int(slot, "id")?, 123);
            assert_eq!(record_page.get_string(slot, "name")?, "test");
            record_page.delete(slot)?;
        }
        tx.commit()?;
        Ok(())
    }
}
