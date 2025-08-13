use crate::{metadata::IndexInfo, query::Constant, record::{schema::FieldType, Layout, RID}, storage::BlockId, tx::Transaction, DbResult};

// Original implementation - https://github.com/redixhumayun/simpledb/blob/master/src/btree.rs

pub struct InternalNodeEntry {
    pub dataval: Constant,
    pub block_num: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageType {
    Internal(Option<usize>),
    Leaf(Option<usize>),
}

impl From<i32> for PageType {
    fn from(value: i32) -> Self {
        const TYPE_MASK: i32 = 1 << 31;
        const VALUE_MASK: i32 = !(1 << 31);
        let is_internal = value & TYPE_MASK == 0;
        if is_internal {
            if value == 0 {
                return PageType::Internal(None);
            } else {
                return PageType::Internal(Some((value & VALUE_MASK) as usize));
            }
        } else {
            let val = value & VALUE_MASK;
            if val == 0 {
                return PageType::Leaf(None);
            } else {
                return PageType::Leaf(Some(val as usize));
            }
        }
    }
}

impl From<PageType> for i32 {
    fn from(value: PageType) -> Self {
        const TYPE_MASK: i32 = 1 << 31;
        match value {
            PageType::Internal(None) => 0,
            PageType::Internal(Some(n)) => n as i32,
            PageType::Leaf(None) => TYPE_MASK,
            PageType::Leaf(Some(n)) => TYPE_MASK | (n as i32),
        }
    }
}

pub struct BTreePage<'tx> {
    tx: Transaction<'tx>,
    block_id: BlockId,
    layout: Layout,
}

impl<'tx> BTreePage<'tx> {
    const INT_BYTES: usize = 4;

    // Column name constants
    // const DATA_VAL_COLUMN: &'static str = "dataval";
    // const BLOCK_NUM_COLUMN: &'static str = "block";
    // const SLOT_NUM_COLUMN: &'static str = "id";

    pub fn new(tx: Transaction<'tx>, block_id: BlockId, layout: Layout) -> DbResult<Self> {
        tx.pin(&block_id)?;
        Ok(Self {
            tx,
            block_id,
            layout,
        })
    }

    /// Calculate the position where the first record having
    /// the specified search key should be, then returns the position before it.
    /// Returns None if the search key belongs at the start of the page
    /// Returns Some(pos) where pos is the index of the rightmost record less than search_key
    pub fn find_slot_before(&self, search_key: &Constant) -> DbResult<Option<usize>> {
        let mut current_slot = 0;
        while current_slot < self.get_number_of_recs()?
            && self.get_data_value(current_slot)? < *search_key
        {
            current_slot += 1;
        }
        if current_slot == 0 {
            return Ok(None);
        } else {
            return Ok(Some(current_slot - 1));
        }
    }

    /// Returns true if adding two more records would exceed the block size
    /// Used primarily for testing to detect splits before they occur
    pub fn is_one_off_full(&self) -> DbResult<bool> {
        let current_records = self.get_number_of_recs()?;
        Ok(self.slot_pos(current_records + 2) > self.tx.block_size())
    }

    /// Returns true if adding one more record would exceed the block size
    pub fn is_full(&self) -> DbResult<bool> {
        let current_records = self.get_number_of_recs()?;
        Ok(self.slot_pos(current_records + 1) > self.tx.block_size())
    }

    /// This method splits the existing [BTreePage] and moves the records from [slot..]
    /// into a new page and then returns the [BlockId] of the new page
    /// The current page continues to be the same, but with fewer records
    pub fn split(&self, slot: usize, page_type: PageType) -> DbResult<BlockId> { 
        let block_id = self.tx.append(&self.block_id.file_name())?;
        let new_btree_page =
            BTreePage::new(self.tx.clone(), block_id.clone(), self.layout.clone())?;
        new_btree_page.format(page_type)?;

        //  set the metadata on the new page
        new_btree_page.set_flag(page_type)?;

        //  move the records from [slot..] to the new page
        let mut dest_slot = 0;
        while slot < self.get_number_of_recs()? {
            new_btree_page.insert(dest_slot)?;
            for field in self.layout.schema().fields() {
                new_btree_page.set_value(dest_slot, field, self.get_value(slot, field)?)?;
            }
            self.delete(slot)?;
            dest_slot += 1;
        }

        Ok(block_id)
    }

    /// Formats a new page by initializing its flag and record count
    /// Sets all record slots to their zero values based on field types
    pub fn format(&self, page_type: PageType) -> DbResult<()> {
        self.tx
            .set_int(&self.block_id, 0, page_type.into(), true)?;
        self.tx.set_int(&self.block_id, Self::INT_BYTES, 0, true)?;
        let record_size = self.layout.slot_size();
        for i in ((2 * Self::INT_BYTES)..self.tx.block_size()).step_by(record_size) {
            for field in self.layout.schema().fields() {
                let field_type = self.layout.schema().field_type(field).unwrap();
                match field_type {
                    FieldType::Integer => {
                        self.tx.set_int(&self.block_id, i, 0, false)?;
                    }
                    FieldType::Varchar => {
                        self.tx.set_string(&self.block_id, i, "", false)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Retrieves the page type flag from the header
    pub fn get_flag(&self) -> DbResult<PageType> {
        self.tx.get_int(&self.block_id, 0).map(PageType::from)
    }

    /// Updates the page type flag in the header
    pub fn set_flag(&self, value: PageType) -> DbResult<()> {
        self.tx.set_int(&self.block_id, 0, value.into(), true)
    }

    /// Gets the data value at the specified slot
    pub fn get_data_value(&self, slot: usize) -> DbResult<Constant> {
        let value = self.get_value(slot, IndexInfo::DATA_FIELD)?;
        Ok(value)
    }

    /// Gets the child block number at the specified slot (for internal nodes)
    fn get_child_block_num(&self, slot: usize) -> DbResult<usize> {
        let block_num = self.get_int(slot, IndexInfo::BLOCK_NUM_FIELD)? as usize;
        Ok(block_num)
    }

    /// Gets the RID stored at the specified slot (for leaf nodes)
    pub fn get_rid(&self, slot: usize) -> DbResult<RID> {
        let block_num = self.get_int(slot, IndexInfo::BLOCK_NUM_FIELD)?;
        let slot_num = self.get_int(slot, IndexInfo::ID_FIELD)? as usize;
        Ok(RID::new(block_num, slot_num))
    }

    /// Inserts a directory entry at the specified slot (for internal nodes)
    /// Directory entries contain a data value and child block number
    fn insert_internal(
        &self,
        slot: usize,
        value: Constant,
        block_num: usize,
    ) -> DbResult<()> {
        self.insert(slot)?;
        self.set_value(slot, IndexInfo::DATA_FIELD, value)?;
        self.set_int(slot, IndexInfo::BLOCK_NUM_FIELD, block_num as i32)?;
        Ok(())
    }

    /// Inserts a leaf entry at the specified slot
    /// Leaf entries contain a data value and RID pointing to the actual record
    pub fn insert_leaf(&self, slot: usize, value: Constant, rid: RID) -> DbResult<()> {
        self.insert(slot)?;
        self.set_value(slot, IndexInfo::DATA_FIELD, value)?;
        self.set_int(slot, IndexInfo::BLOCK_NUM_FIELD, rid.block_number() as i32)?;
        self.set_int(slot, IndexInfo::ID_FIELD, rid.slot() as i32)?;
        Ok(())
    }

    /// Inserts space for a new record at the specified slot
    /// Shifts all following records right by one position
    fn insert(&self, slot: usize) -> DbResult<()> {
        let current_records = self.get_number_of_recs()?;
        for i in (slot..current_records).rev() {
            //  move records over by one to the right
            self.copy_record(i, i + 1)?;
        }
        self.set_number_of_recs(current_records + 1)?;
        Ok(())
    }

    /// Deletes the record at the specified slot
    /// Shifts all following records left by one position
    pub fn delete(&self, slot: usize) -> DbResult<()> {
        let current_records = self.get_number_of_recs()?;
        for i in slot + 1..current_records {
            self.copy_record(i, i - 1)?;
        }
        self.set_number_of_recs(current_records - 1)?;
        Ok(())
    }

    /// Copies all fields from one record slot to another
    fn copy_record(&self, from: usize, to: usize) -> DbResult<()> {
        for field in self.layout.schema().fields() {
            self.set_value(to, field, self.get_value(from, field)?)?;
        }
        Ok(())
    }

    /// Gets the number of records currently stored in the page
    pub fn get_number_of_recs(&self) -> DbResult<usize> {
        self.tx
            .get_int(&self.block_id, Self::INT_BYTES)
            .map(|v| v as usize)
    }

    /// Updates the number of records stored in the page
    fn set_number_of_recs(&self, num: usize) -> DbResult<()> {
        self.tx
            .set_int(&self.block_id, Self::INT_BYTES, num as i32, true)
    }

    fn get_int(&self, slot: usize, field_name: &str) -> DbResult<i32> {
        self.tx.get_int(
            &self.block_id,
            self.slot_pos(slot) + self.layout.offset(field_name).unwrap(),
        )
    }

    fn set_int(&self, slot: usize, field_name: &str, value: i32) -> DbResult<()> {
        self.tx.set_int(
            &self.block_id,
            self.field_position(slot, field_name),
            value,
            true,
        )
    }

    fn get_string(&self, slot: usize, field_name: &str) -> DbResult<String> {
        self.tx.get_string(
            &self.block_id,
            self.slot_pos(slot) + self.layout.offset(field_name).unwrap(),
        )
    }

    fn set_string(
        &self,
        slot: usize,
        field_name: &str,
        value: String,
    ) -> DbResult<()> {
        self.tx.set_string(
            &self.block_id,
            self.field_position(slot, field_name),
            &value,
            true,
        )
    }

    fn get_value(&self, slot: usize, field_name: &str) -> DbResult<Constant> {
        let field_type = self
            .layout
            .schema()
            .field_type(field_name)
            .ok_or_else(|| format!("Field {} not found in schema", field_name))
            .unwrap();
        match field_type {
            FieldType::Integer => {
                let value = self.get_int(slot, field_name)?;
                Ok(Constant::Int(value))
            }
            FieldType::Varchar => {
                let value = self.get_string(slot, field_name)?;
                Ok(Constant::String(value))
            }
        }
    }

    fn set_value(
        &self,
        slot: usize,
        field_name: &str,
        value: Constant,
    ) -> DbResult<()> {
        let expected_type = self
            .layout
            .schema()
            .field_type(field_name)
            .ok_or_else(|| format!("Field {} not found in schema", field_name))
            .unwrap();

        // Check if value type matches schema
        match (expected_type, &value) {
            (FieldType::Integer, Constant::Int(v)) => self.set_int(slot, field_name, *v),
            (FieldType::Varchar, Constant::String(v)) => {
                self.set_string(slot, field_name, v.clone())
            }
            _ => Err(crate::DbError::LockAbort), // TODO
        }
    }

    /// Calculates the byte position of a field within a record slot
    fn field_position(&self, slot: usize, field_name: &str) -> usize {
        self.slot_pos(slot) + self.layout.offset(field_name).unwrap()
    }

    /// Calculates the starting byte position of a record slot
    fn slot_pos(&self, slot: usize) -> usize {
        Self::INT_BYTES + Self::INT_BYTES + slot * self.layout.slot_size()
    }

    /// Unpins the page's block from the buffer manager
    fn close(&self) {
        self.tx.unpin(&self.block_id);
    }
}

impl Drop for BTreePage<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Display for BTreePage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== BTreePage Debug ===")?;
        writeln!(f, "Block: {:?}", self.block_id)?;
        match self.get_flag() {
            Ok(flag) => writeln!(f, "Page Type: {:?}", flag)?,
            Err(e) => writeln!(f, "Error getting flag: {}", e)?,
        }

        match self.get_number_of_recs() {
            Ok(count) => {
                writeln!(f, "Record Count: {}", count)?;
                writeln!(f, "Entries:")?;
                match self.get_flag() {
                    Ok(PageType::Internal(_)) => {
                        for slot in 0..count {
                            if let (Ok(key), Ok(child)) =
                                (self.get_data_value(slot), self.get_child_block_num(slot))
                            {
                                writeln!(f, "Slot {}: Key={:?}, Child Block={}", slot, key, child)?;
                            }
                        }
                    }
                    Ok(PageType::Leaf(_)) => {
                        for slot in 0..count {
                            if let (Ok(key), Ok(rid)) =
                                (self.get_data_value(slot), self.get_rid(slot))
                            {
                                writeln!(
                                    f,
                                    "Slot {}: Key={:?}, RID=(block={}, slot={})",
                                    slot, key, rid.block_number(), rid.slot()
                                )?;
                            }
                        }
                    }
                    Err(e) => writeln!(f, "Error getting page type: {}", e)?,
                }
            }
            Err(e) => writeln!(f, "Error getting record count: {}", e)?,
        }
        writeln!(f, "====================")
    }
}

#[cfg(test)]
mod btree_page_tests {
    use crate::{record::Schema, utils::testing_utils::temp_db};

    use super::*;

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::DATA_FIELD);
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        Layout::new(schema)
    }

    #[test]
    fn test_btree_page_format() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;
        let block = tx.append("test")?;
        let layout = create_test_layout();

        let page = BTreePage::new(tx.clone(), block, layout)?;
        page.format(PageType::Leaf(None))?;

        assert_eq!(page.get_flag()?, PageType::Leaf(None));
        assert_eq!(page.get_number_of_recs()?, 0);
        Ok(())
    }

    #[test]
    fn test_leaf_insert_and_delete() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;
        let block = tx.append("test")?;
        let layout = create_test_layout();

        let page = BTreePage::new(tx.clone(), block, layout)?;
        page.format(PageType::Leaf(None))?;

        let rid = RID::new(1, 1);
        page.insert_leaf(0, Constant::Int(10), rid.clone())?;

        assert_eq!(page.get_number_of_recs()?, 1);
        assert_eq!(page.get_data_value(0)?, Constant::Int(10));
        assert_eq!(page.get_rid(0)?, rid);

        page.delete(0)?;
        assert_eq!(page.get_number_of_recs().unwrap(), 0);
        Ok(())
    }

    #[test]
    fn test_page_split() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;
        let block = tx.append("test")?;
        let layout = create_test_layout();

        let page = BTreePage::new(tx.clone(), block.clone(), layout.clone())?;
        page.format(PageType::Leaf(None)).unwrap();

        // Insert records until full
        let mut slot = 0;
        while !page.is_full().unwrap() {
            page.insert_leaf(slot, Constant::Int(slot as i32), RID::new(1, slot))
                .unwrap();
            slot += 1;
        }

        // Split the page
        let split_point = slot / 2;
        let new_block = page.split(split_point, PageType::Leaf(None)).unwrap();

        // Verify original page
        assert_eq!(page.get_number_of_recs().unwrap(), split_point);

        // Verify new page
        let new_page = BTreePage::new(tx.clone(), new_block, layout)?;
        assert_eq!(new_page.get_number_of_recs().unwrap(), slot - split_point);
        Ok(())
    }

    #[test]
    fn test_type_safety() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;
        let block = tx.append("test")?;
        let layout = create_test_layout();

        let page = BTreePage::new(tx.clone(), block, layout)?;
        page.format(PageType::Leaf(None))?;

        // Try to insert wrong type
        let result = page.set_value(0, "dataval", Constant::String("wrong type".to_string()));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_internal_node_operations() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;
        let block = tx.append("test")?;
        let layout = create_test_layout();

        let page = BTreePage::new(tx.clone(), block, layout)?;
        page.format(PageType::Internal(None))?;

        // Insert internal entry
        page.insert_internal(0, Constant::Int(10), 2).unwrap();

        // Verify entry
        assert_eq!(page.get_data_value(0).unwrap(), Constant::Int(10));
        assert_eq!(page.get_child_block_num(0).unwrap(), 2);

        Ok(())
    }

    #[test]
    fn test_find_slot_before() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;
        let block = tx.append("test")?;
        let layout = create_test_layout();

        let page = BTreePage::new(tx.clone(), block, layout)?;
        page.format(PageType::Leaf(None))?;

        page.insert_leaf(0, Constant::Int(10), RID::new(1, 1))?;
        page.insert_leaf(1, Constant::Int(20), RID::new(1, 2))?;
        page.insert_leaf(2, Constant::Int(30), RID::new(1, 3))?;

        assert_eq!(
            page.find_slot_before(&Constant::Int(15))?.unwrap(),
            0
        );
        assert_eq!(
            page.find_slot_before(&Constant::Int(20))?.unwrap(),
            0
        );
        assert_eq!(
            page.find_slot_before(&Constant::Int(25))?.unwrap(),
            1
        );
        Ok(())
    }
}