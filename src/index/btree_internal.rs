use crate::{index::{btree_page::{InternalNodeEntry, PageType}, BTreePage}, query::Constant, record::Layout, storage::BlockId, tx::Transaction, DbResult};

// Original implementation - https://github.com/redixhumayun/simpledb/blob/master/src/btree.rs

pub struct BTreeInternal<'tx> {
    txn: Transaction<'tx>,
    block_id: BlockId,
    layout: Layout,
    pub contents: BTreePage<'tx>,
    file_name: String,
}

impl std::fmt::Display for BTreeInternal<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== BTreeInternal Debug ===")?;
        writeln!(f, "Block ID: {:?}", self.block_id)?;
        writeln!(f, "File Name: {}", self.file_name)?;
        writeln!(f, "\nContents:")?;
        write!(f, "{}", self.contents)?;
        Ok(())
    }
}

impl<'tx> BTreeInternal<'tx> {
    pub fn new(txn: Transaction<'tx>, block_id: BlockId, layout: Layout, file_name: String) -> DbResult<Self> {
        let contents = BTreePage::new(txn.clone(), block_id.clone(), layout.clone())?;
        Ok(Self {
            txn,
            block_id,
            layout,
            contents,
            file_name,
        })
    }

    /// This method will search for a given key in the [BTreeInternal] node
    /// It will find the child block that contains the key
    /// It will return the block ID of the child block
    pub fn search(&mut self, search_key: &Constant) -> DbResult<usize> {
        let mut child_block = self.find_child_block(search_key)?;
        while !matches!(self.contents.get_flag()?, PageType::Internal(None)) {
            self.contents = BTreePage::new(
                self.txn.clone(),
                child_block.clone(),
                self.layout.clone(),
            )?;
            child_block = self.find_child_block(search_key)?;
        }
        Ok(child_block.number() as usize)
    }

    /// This method will create a new root for the BTree
    /// It will take the entry that needs to be inserted after the split, move its existing
    /// entries into a new block and then insert both the newly created block with its old entries and the new block
    /// This is done so that the root is always at block 0 of the file
    pub fn make_new_root(&self, entry: InternalNodeEntry) -> DbResult<()> {
        let first_value = self.contents.get_data_value(0)?;
        let page_type = self.contents.get_flag()?;
        let level = match page_type {
            PageType::Internal(None) => 0,
            PageType::Internal(Some(n)) => n,
            _ => panic!("Invalid page type for new root"),
        };
        let new_block_id = self.contents.split(0, page_type)?;
        let new_block_entry = InternalNodeEntry {
            dataval: first_value,
            block_num: new_block_id.number() as usize,
        };
        self.insert_entry(new_block_entry)?;
        self.insert_entry(entry)?;
        self.contents
            .set_flag(PageType::Internal(Some(level + 1)))?;
        Ok(())
    }

    /// This method will insert a new entry into the [BTreeInternal] node
    /// It works in conjunction with [BTreeInternal::insert_internal_node_entry] to do the insertion
    /// This method will find the correct child block to insert it into and the [BTreeInternal::insert_internal_node_entry] will do the actual
    /// insertion into the specific block
    pub fn insert_entry(
        &self,
        entry: InternalNodeEntry,
    ) -> DbResult<Option<InternalNodeEntry>> {
        if matches!(self.contents.get_flag()?, PageType::Internal(None)) {
            return self.insert_internal_node_entry(entry);
        }
        let child_block = self.find_child_block(&entry.dataval)?;
        let child_internal_node = BTreeInternal::new(
            self.txn.clone(),
            child_block,
            self.layout.clone(),
            self.file_name.clone(),
        )?;
        let new_entry = child_internal_node.insert_entry(entry)?;
        match new_entry {
            Some(entry) => {
                return self.insert_internal_node_entry(entry);
            }
            None => return Ok(None),
        }
    }

    /// This method will insert a new entry into the [BTreeInternal] node
    /// It will find the appropriate slot for the new entry
    /// If the page is full, it will split the page and return the new entry
    fn insert_internal_node_entry(
        &self,
        entry: InternalNodeEntry,
    ) -> DbResult<Option<InternalNodeEntry>> {
        let slot = match self.contents.find_slot_before(&entry.dataval)? {
            Some(slot) => slot + 1, //  move to the insertion point
            None => 0,              //  the insertion point is at the first slot
        };
        self.contents.insert_internal(slot, entry.dataval, entry.block_num)?;

        if !self.contents.is_full()? {
            return Ok(None);
        }

        let page_type = self.contents.get_flag()?;
        let split_point = self.contents.get_number_of_recs()? / 2;
        let split_record = self.contents.get_data_value(split_point)?;
        let new_block_id = self.contents.split(split_point, page_type)?;
        return Ok(Some(InternalNodeEntry {
            dataval: split_record,
            block_num: new_block_id.number() as usize,
        }));
    }

    /// This method will find the child block for a given search key in a [BTreeInternal] node
    /// It will search for the rightmost slot before the search key
    /// If the search key is found in the slot, it will return the next slot
    fn find_child_block(&self, search_key: &Constant) -> DbResult<BlockId> {
        let mut slot = match self.contents.find_slot_before(&search_key)? {
            Some(slot) => slot,
            None => 0,
        };
        if self.contents.get_data_value(slot + 1)? == *search_key {
            slot += 1;
        }
        let block_num = self.contents.get_child_block_num(slot)?;
        Ok(BlockId::new(self.file_name.clone(), block_num as i32))
    }
}

#[cfg(test)]
mod tests {
    use crate::{metadata::IndexInfo, record::Schema, utils::testing_utils::temp_db, SimpleDB};

    use super::*;

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::DATA_FIELD);
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        Layout::new(schema)
    }

    fn setup_internal_node<'tx>(db: &'tx SimpleDB<'tx>) -> DbResult<(Transaction<'tx>, BTreeInternal<'tx>)> {
        let tx = db.new_tx()?;
        let block = tx.append("test")?;
        let layout = create_test_layout();

        // Format the page as internal node
        let page = BTreePage::new(tx.clone(), block.clone(), layout.clone())?;
        page.format(PageType::Internal(None)).unwrap();

        let internal = BTreeInternal::new(tx.clone(), block, layout, "test".to_string())?;
        Ok((tx, internal))
    }

    #[test]
    fn test_search_simple_path() -> DbResult<()> {
        let db = temp_db()?;
        let (_, internal) = setup_internal_node(&db)?;

        // Insert some entries to create a simple path
        internal
            .contents
            .insert_internal(0, Constant::Int(10), 2)
            .unwrap();
        internal
            .contents
            .insert_internal(1, Constant::Int(20), 3)
            .unwrap();
        internal
            .contents
            .insert_internal(2, Constant::Int(30), 4)
            .unwrap();

        // Search for a value - should return correct child block
        let result = internal.find_child_block(&Constant::Int(15)).unwrap();
        assert_eq!(result.number(), 2); // Should return block 2 since 15 < 20

        let result = internal.find_child_block(&Constant::Int(25)).unwrap();
        assert_eq!(result.number(), 3); // Should return block 3 since 20 < 25 < 30
        Ok(())
    }

    #[test]
    fn test_insert_with_split() -> DbResult<()> {
        let db = temp_db()?;
        let (_, internal) = setup_internal_node(&db)?;

        // Fill the node until just before splitting
        let mut block_num = 0;
        while !internal.contents.is_one_off_full().unwrap() {
            let entry = InternalNodeEntry {
                dataval: Constant::Int(block_num),
                block_num: block_num as usize,
            };
            internal.insert_entry(entry).unwrap();
            block_num += 1;
        }

        // Insert one more entry to force split
        let entry = InternalNodeEntry {
            dataval: Constant::Int(block_num),
            block_num: block_num as usize,
        };

        let split_result = internal.insert_entry(entry).unwrap();
        assert!(split_result.is_some());

        let split_entry = split_result.unwrap();
        assert!(split_entry.block_num > 0); // Should be a new block number

        // Verify middle key was chosen for split
        let mid_val = ((block_num + 1) / 2) as i32;
        assert_eq!(split_entry.dataval, Constant::Int(mid_val));
        Ok(())
    }

    #[test]
    fn test_make_new_root() -> DbResult<()> {
        let db = temp_db()?;
        let (_, internal) = setup_internal_node(&db)?;

        // Setup initial entries
        internal
            .contents
            .insert_internal(0, Constant::Int(10), 2)
            .unwrap();
        internal
            .contents
            .insert_internal(1, Constant::Int(20), 3)
            .unwrap();

        // Create a new entry that will be part of new root
        let new_entry = InternalNodeEntry {
            dataval: Constant::Int(30),
            block_num: 4,
        };

        // Make new root
        internal.make_new_root(new_entry).unwrap();

        // Verify root structure
        assert!(matches!(
            internal.contents.get_flag().unwrap(),
            PageType::Internal(Some(1))
        ));
        assert_eq!(internal.contents.get_number_of_recs().unwrap(), 2);

        // First entry should point to block with original entries
        assert!(internal.contents.get_child_block_num(0).unwrap() > 0);
        // Second entry should be our new entry
        assert_eq!(internal.contents.get_child_block_num(1).unwrap(), 4);
        Ok(())
    }

    #[test]
    fn test_insert_recursive_split() -> DbResult<()> {
        let db = temp_db()?;
        let (_, mut internal) = setup_internal_node(&db)?;

        // Create a multi-level tree by filling and splitting nodes
        let mut value = 1;
        while !internal.contents.is_one_off_full().unwrap() {
            let entry = InternalNodeEntry {
                dataval: Constant::Int(value),
                block_num: value as usize,
            };
            internal.insert_entry(entry).unwrap();
            value += 1;
        }

        // Insert one more to force recursive split
        let entry = InternalNodeEntry {
            dataval: Constant::Int(value),
            block_num: value as usize,
        };

        let split_result = internal.insert_entry(entry).unwrap();
        assert!(split_result.is_some());

        // Verify the split maintained tree properties
        let leaf_block_num = internal.search(&Constant::Int(3)).unwrap();
        assert!(leaf_block_num > 0);
        Ok(())
    }

    #[test]
    fn test_edge_cases() -> DbResult<()> {
        let db = temp_db()?;
        let (_, internal) = setup_internal_node(&db)?;

        // Test inserting duplicate keys
        internal
            .insert_entry(InternalNodeEntry {
                dataval: Constant::Int(10),
                block_num: 1,
            })
            .unwrap();
        internal
            .insert_entry(InternalNodeEntry {
                dataval: Constant::Int(10),
                block_num: 2,
            })
            .unwrap();

        println!("the page contents {}", internal.contents);

        //  NOTE: It looks like the numbers are reversed here in the sense that the block numbers asserted are backwards
        //  but they are correct because the insertion into the node results in a page that looks like this where block 2
        //  is in slot 0
        //  === BTreePage Debug ===
        //  Block: BlockId { filename: "test_file_1746190249550660000_ThreadId(2)", block_num: 0 }
        //  Page Type: Internal(None)
        //  Record Count: 2
        //  Entries:
        //  Slot 0: Key=Int(10), Child Block=2
        //  Slot 1: Key=Int(10), Child Block=1
        //  ====================
        // Search should return the rightmost child for duplicate key
        let result = internal.find_child_block(&Constant::Int(10)).unwrap();
        assert_eq!(result.number(), 1);

        // Test searching for key less than all entries
        let result = internal.find_child_block(&Constant::Int(5)).unwrap();
        assert_eq!(result.number(), 2);

        // Test searching for key greater than all entries
        let result = internal.find_child_block(&Constant::Int(15)).unwrap();
        assert_eq!(result.number(), 1);
        Ok(())
    }
}