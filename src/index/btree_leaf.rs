use crate::{index::{btree_page::{InternalNodeEntry, PageType}, BTreePage}, query::Constant, record::{Layout, RID}, storage::BlockId, tx::Transaction, DbResult};

// Original implementation - https://github.com/redixhumayun/simpledb/blob/master/src/btree.rs

pub struct BTreeLeaf<'tx> {
    tx: Transaction<'tx>,
    layout: Layout,
    search_key: Constant,
    contents: BTreePage<'tx>,
    current_slot: Option<usize>,
    file_name: String,
}

impl std::fmt::Display for BTreeLeaf<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n============= LEAF {:?} ===", self.contents.block_id())?;
        writeln!(f, "Search Key: {:?}", self.search_key)?;
        writeln!(f, "Current Slot: {:?}", self.current_slot)?;
        writeln!(f, "\nContents:")?;
        write!(f, "{}", self.contents)?;
        Ok(())
    }
}

impl<'tx> BTreeLeaf<'tx> {
    pub fn new(
        tx: Transaction<'tx>,
        block_id: BlockId,
        layout: Layout,
        search_key: Constant,
        file_name: String,
    ) -> DbResult<Self> {
        let contents = BTreePage::new(tx.clone(), block_id, layout.clone())?;
        let current_slot = contents.find_slot_before(&search_key)?;
        Ok(Self {
            tx,
            layout,
            search_key,
            contents,
            current_slot,
            file_name,
        })
    }

    /// Advances to the next record that matches the search key
    /// If we've reached the end of the current page, attempts to follow the overflow chain
    /// Returns Some(()) if a matching record is found, None otherwise
    pub fn next(&mut self) -> DbResult<Option<()>> {
        self.current_slot = {
            match self.current_slot {
                Some(slot) => Some(slot + 1),
                None => Some(0),
            }
        };
        if self.current_slot.unwrap() >= self.contents.get_number_of_recs()? {
            return self.try_overflow();
        } else if self.contents.get_data_value(self.current_slot.unwrap())? == self.search_key {
            return Ok(Some(()));
        } else {
            return self.try_overflow();
        }
    }

    /// Deletes the record with the specified RID from this leaf page or its overflow chain
    /// Returns Ok(()) if the record was found and deleted, error otherwise
    /// Requires that current_slot is initialized
    pub fn delete(&mut self, rid: RID) -> DbResult<()> {
        while let Some(_) = self.next()? {
            if self.contents.get_rid(self.current_slot.unwrap())? == rid {
                self.contents.delete(self.current_slot.unwrap())?;
                return Ok(());
            }
        }
        return Err(crate::DbError::NotFound);
    }

    /// This method will attempt to insert an entry into a [BTreeLeaf] page
    /// If the leaf page has an overflow page, and the new entry is smaller than the first key, split the page
    /// If the page splits, return the [InternalNodeEntry] identifier to the new page
    pub fn insert(&mut self, rid: RID) -> DbResult<Option<InternalNodeEntry>> {
        //  If this page has an overflow page, and the key being inserted is less than the first key force a split
        //  This is done to ensure that overflow pages are linked to a page with the first key the same as entries in overflow pages
        if matches!(self.contents.get_flag()?, PageType::Leaf(Some(_)))
            && self.contents.get_data_value(0)? > self.search_key
        {
            let first_entry = self.contents.get_data_value(0)?;
            let new_block_id = self.contents.split(0, self.contents.get_flag()?)?;
            self.current_slot = Some(0);
            self.contents.set_flag(PageType::Leaf(None))?;
            self.contents.insert_leaf(0, self.search_key.clone(), rid)?;
            return Ok(Some(InternalNodeEntry {
                dataval: first_entry,
                block_num: new_block_id.number() as usize,
            }));
        }

        self.current_slot = {
            match self.current_slot {
                Some(slot) => Some(slot + 1),
                None => Some(0),
            }
        };
        self.contents
            .insert_leaf(self.current_slot.unwrap(), self.search_key.clone(), rid)?;
        if !self.contents.is_full()? {
            return Ok(None);
        }

        //  The leaf needs to be split. There are two cases to handle here
        //
        //  The page is full of identical keys
        //  1. Create an overflow block and move all keys except the first key there
        //  2. Link the current page to the overflow block
        //
        //  The page is not full of identical keys
        //  1. Find the split point
        //  2. Move the split point
        //
        //  Moving the split point
        //  If the split key is identical to the first key, move it right because all identical keys need to stay together
        //  If the split key is not identical to the first key, move it left until the the first instance of the split key is found
        let first_key = self.contents.get_data_value(0)?;
        let last_key = self
            .contents
            .get_data_value(self.contents.get_number_of_recs()? - 1)?;
        if first_key == last_key {
            let new_block_id = self.contents.split(1, self.contents.get_flag()?)?;
            self.contents
                .set_flag(PageType::Leaf(Some(new_block_id.number() as usize)))?;
            return Ok(None);
        }

        let mut split_point = self.contents.get_number_of_recs()? / 2;
        let mut split_record = self.contents.get_data_value(split_point)?;
        if split_record == first_key {
            while self.contents.get_data_value(split_point)? == first_key {
                split_point += 1;
            }
            split_record = self.contents.get_data_value(split_point)?;
        } else {
            while self.contents.get_data_value(split_point - 1)? == split_record {
                split_point -= 1;
            }
        }
        let new_block_id = self.contents.split(split_point, PageType::Leaf(None))?;

        Ok(Some(InternalNodeEntry {
            dataval: split_record,
            block_num: new_block_id.number() as usize,
        }))
    }

    /// This method will check to see if an overflow page is present for this block
    /// An overflow page for a specific page will contain entries that are the same as the first key of the current page
    /// If no overflow page can be found, just return. Otherwise swap out the current contents for the overflow contents
    fn try_overflow(&mut self) -> DbResult<Option<()>> {
        let first_key = self.contents.get_data_value(0)?;

        if first_key != self.search_key
            || !matches!(self.contents.get_flag()?, PageType::Leaf(Some(_)))
        {
            return Ok(None);
        }

        let PageType::Leaf(Some(overflow_block_num)) = self.contents.get_flag()? else {
            return Ok(None);
        };

        let overflow_contents = BTreePage::new(
            self.tx.clone(),
            BlockId::new(self.file_name.clone(), overflow_block_num as i32),
            self.layout.clone(),
        )?;
        self.contents = overflow_contents;
        Ok(Some(()))
    }

    pub fn get_data_rid(&self) -> DbResult<RID> {
        self.contents.get_rid(
            self.current_slot
                .expect("Current slot not set in BTreeLeaf::get_data_rid"),
        )
    }
}

#[cfg(test)]
mod btree_leaf_tests {
    use crate::{metadata::IndexInfo, record::Schema, utils::testing_utils::temp_db, SimpleDB};

    use super::*;

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::DATA_FIELD);
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        Layout::new(schema)
    }

    fn setup_leaf<'tx>(db: &'tx SimpleDB, search_key: Constant) -> DbResult<(Transaction<'tx>, BTreeLeaf<'tx>)> {
        let tx = db.new_tx()?;
        let block = tx.append("testfile")?;
        let layout = create_test_layout();

        let page = BTreePage::new(tx.clone(), block.clone(), layout.clone())?;
        page.format(PageType::Leaf(None))?;

        let leaf = BTreeLeaf::new(
            tx.clone(),
            block,
            layout,
            search_key,
            "testfile".to_string(),
        )?;

        Ok((tx, leaf))
    }

    #[test]
    fn test_insert_no_split() -> DbResult<()> {
        let db = temp_db()?;
        let (_tx, mut leaf) = setup_leaf(&db, Constant::Int(10))?;

        // Insert should succeed without splitting
        assert!(leaf.insert(RID::new(1, 1)).unwrap().is_none());

        // Verify the record was inserted
        assert_eq!(leaf.contents.get_number_of_recs().unwrap(), 1);
        assert_eq!(leaf.contents.get_data_value(0).unwrap(), Constant::Int(10));
        Ok(())
    }

    #[test]
    fn test_insert_with_split_different_keys() -> DbResult<()> {
        let db = temp_db()?;
        let (_tx, mut leaf) = setup_leaf(&db, Constant::Int(10))?;

        // Fill the page with different keys
        let mut slot = 0;
        // let mut split_result = None;
        while !leaf.contents.is_one_off_full()? {
            leaf.search_key = Constant::Int(slot);
            leaf.insert(RID::new(1, slot as usize)).unwrap();
            slot += 1;
        }

        let split_result = leaf.insert(RID::new(1, slot as usize)).unwrap();

        // Verify split occurred
        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.block_num, 1); //  this is a new file that has just added a new block
        assert_eq!(entry.dataval, Constant::Int((slot + 1) / 2)); // Middle key. Adding 1 to slot because slot is 0-based
        Ok(())
    }

    #[test]
    fn test_insert_with_overflow_same_keys() -> DbResult<()> {
        let db = temp_db()?;
        let (_tx, mut leaf) = setup_leaf(&db, Constant::Int(10))?;

        // Fill the page with same key
        let mut slot = 0;
        while !leaf.contents.is_one_off_full().unwrap() {
            leaf.insert(RID::new(1, slot)).unwrap();
            slot += 1;
        }

        // Insert one more record with same key to force overflow
        let split_result = leaf.insert(RID::new(1, slot)).unwrap();

        // Verify overflow block was created
        assert!(split_result.is_none()); //  overflow block returns None
        let PageType::Leaf(Some(overflow_num)) = leaf.contents.get_flag().unwrap() else {
            panic!("Expected overflow block");
        };

        // Verify first key matches in both pages
        assert_eq!(leaf.contents.get_data_value(0).unwrap(), Constant::Int(10));

        Ok(())
    }

    #[test]
    fn test_insert_with_existing_overflow() -> DbResult<()> {
        let db = temp_db()?;
        let (_tx, mut leaf) = setup_leaf(&db, Constant::Int(10))?;

        // Create a page with overflow block containing key 10
        leaf.search_key = Constant::Int(10);
        let mut slot = 0;
        while !leaf.contents.is_one_off_full().unwrap() {
            leaf.insert(RID::new(1, slot)).unwrap();
            slot += 1;
        }
        leaf.insert(RID::new(1, slot)).unwrap(); // Create overflow with split

        // Try to insert key 5 (less than 10) which will force another split
        leaf.search_key = Constant::Int(5);
        let split_result = leaf.insert(RID::new(2, 1)).unwrap();

        // Verify page was split
        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.dataval, Constant::Int(10));

        Ok(())
    }

    #[test]
    fn test_insert_edge_cases() -> DbResult<()> {
        let db = temp_db()?;
        
        // Test case 1: Insert when split point equals first key
        let (_, mut leaf) = setup_leaf(&db, Constant::Int(10))?;
        // Fill page with alternating 10s and 20s
        let mut counter = 0;
        while !leaf.contents.is_one_off_full().unwrap() {
            leaf.search_key = Constant::Int(if counter % 2 == 0 { 10 } else { 20 });
            leaf.insert(RID::new(1, counter)).unwrap();
            counter += 1;
        }

        // Force a split - should move split point right until after all 10s
        leaf.search_key = Constant::Int(15);
        let split_result = leaf.insert(RID::new(1, 10)).unwrap();
        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.dataval, Constant::Int(20)); // First non-10 value

        Ok(())
    }
}