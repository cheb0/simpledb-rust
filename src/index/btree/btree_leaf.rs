use crate::error::DbResult;
use crate::record::{Layout, RID};
use crate::storage::BlockId;
use crate::tx::Transaction;
use crate::query::Constant;

use super::bt_page::BTPage;

pub struct DirEntry {
    dataval: Constant,
    block_num: i32,
}

impl DirEntry {
    pub fn new(dataval: Constant, block_num: i32) -> Self {
        Self {
            dataval, 
            block_num
        }
    }
}

/// Represents a B+Tree leaf page that contains index records.
/// The leaf pages store the actual data RIDs and are linked together
/// for range queries.
pub struct BTreeLeaf<'a> {
    tx: Transaction<'a>,
    layout: Layout,
    search_key: Constant,
    contents: BTPage<'a>,
    current_slot: i32,
    filename: String,
}

impl<'a> BTreeLeaf<'a> {
    /// Opens a buffer to hold the specified leaf block.
    /// The buffer is positioned immediately before the first record
    /// having the specified search key (if any).
    /// 
    /// # Arguments
    /// * `tx` - The calling transaction
    /// * `blk` - A reference to the disk block
    /// * `layout` - The metadata of the B-tree leaf file
    /// * `search_key` - The search key value
    pub fn new(
        tx: Transaction<'a>,
        blk: BlockId,
        layout: Layout,
        search_key: Constant,
    ) -> DbResult<Self> {
        let contents = BTPage::new(tx.clone(), blk.clone(), layout.clone())?;
        let current_slot = contents.find_slot_before(&search_key)?;
        let filename = blk.file_name();

        Ok(BTreeLeaf {
            tx,
            layout,
            search_key,
            contents,
            current_slot,
            filename: filename.to_string(),
        })
    }

   /// Moves to the next leaf record having the previously-specified search key.
   /// Returns `false` if there are no more leaf records for the search key
    pub fn next(&mut self) -> DbResult<bool> {
        self.current_slot += 1;
        if self.current_slot as usize >= self.contents().records_cnt()? {
            self.try_overflow()
        } else if self.contents.get_data_val(self.current_slot as usize)? == self.search_key {
            Ok(true)
        } else {
            self.try_overflow()
        }
    }

    /// Insert the specified record into the index. The method first traverses 
    /// the directory to find the appropriate leaf page; then it inserts
    /// the record into the leaf. If the insertion causes the leaf to split, then
    /// the method calls insert on the root, passing it the directory entry of 
    /// the new leaf page. If the root node splits, then makeNewRoot is called.
    pub fn insert(&mut self, rid: RID) -> DbResult<Option<DirEntry>> {
        if self.contents.get_flag()? >= 0 
        && self.contents.get_data_val(0)?.compare_to(&self.search_key) == std::cmp::Ordering::Greater {
            let first_val = self.contents.get_data_val(0)?;
            let new_blk = self.contents.split(0, self.contents.get_flag()?)?;
            self.current_slot = 0;
            self.contents.set_flag(-1)?;
            self.contents.insert_leaf(self.current_slot as usize, &self.search_key, &rid)?;
            return Ok(Some(DirEntry::new(first_val, new_blk.number())));
        }

        self.current_slot += 1;
        self.contents.insert_leaf(self.current_slot as usize, &self.search_key, &rid)?;
        if !self.contents.is_full()? {
            return Ok(None);
        }
        panic!("B+Tree node is full, split is not supported");
    }

    /// Attempts to move to the next leaf page if the current page
    /// is an overflow page and the search key matches the first key.
    /// Returns
    /// * `true` if successfully moved to overflow page
    /// * `false` if no overflow or search key doesn't match
    fn try_overflow(&mut self) -> DbResult<bool> {
        // Get the first key in the current page
        let first_key = self.contents.get_data_val(0)?;
        let flag = self.contents.get_flag()?;

        // Check if search key matches first key and flag indicates overflow
        if self.search_key != first_key || flag < 0 {
            return Ok(false);
        }

        let next_blk = BlockId::new(self.filename.clone(), flag);
        self.contents = BTPage::new(self.tx.clone(), next_blk, self.layout.clone())?;
        self.current_slot = 0;
        
        Ok(true)
    }

    pub fn current_slot(&self) -> i32 {
        self.current_slot
    }

    pub fn search_key(&self) -> &Constant {
        &self.search_key
    }

    pub fn contents(&self) -> &BTPage<'a> {
        &self.contents
    }

    pub fn contents_mut(&mut self) -> &mut BTPage<'a> {
        &mut self.contents
    }
}

impl<'a> Drop for BTreeLeaf<'a> {
    fn drop(&mut self) {
        // BTPage will handle unpinning in its own Drop implementation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Schema;
    use crate::utils::testing_utils::temp_db;

    #[test]
    fn test_btree_leaf_creation() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field("dataval", 10);
        schema.add_int_field("block");
        schema.add_int_field("id");

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        let search_key = Constant::string("test_key");

        let leaf = BTreeLeaf::new(tx.clone(), blk, layout, search_key.clone())?;

        assert_eq!(leaf.search_key(), &search_key);
        assert_eq!(leaf.current_slot(), -1); // Should be before first record

        Ok(())
    }

    #[test]
    fn test_btree_leaf_with_existing_data() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field("dataval", 5);
        schema.add_int_field("block");
        schema.add_int_field("id");

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        
        // Create a page and insert some data
        let page = BTPage::new(tx.clone(), blk.clone(), layout.clone())?;
        page.insert_leaf(0, &Constant::string("AAAAA"), &crate::record::RID::new(1, 1))?;
        page.insert_leaf(1, &Constant::string("BBBBB"), &crate::record::RID::new(2, 2))?;
        page.insert_leaf(2, &Constant::string("CCCCC"), &crate::record::RID::new(3, 3))?;
        drop(page); // Explicitly drop to unpin

        // Create leaf with search key
        let search_key = Constant::string("BBBBB");
        let leaf = BTreeLeaf::new(tx.clone(), blk, layout, search_key)?;

        // Should be positioned at slot 0 (before BBBBB)
        assert_eq!(leaf.current_slot(), 0);

        Ok(())
    }

    #[test]
    fn test_btree_leaf_try_overflow() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field("dataval", 8);
        schema.add_int_field("block");
        schema.add_int_field("id");

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        tx.append("testindex")?;
        
        let page = BTPage::new(tx.clone(), blk.clone(), layout.clone())?;
        page.set_flag(1)?; // Set overflow block number
        page.insert_leaf(0, &Constant::string("test_key"), &crate::record::RID::new(1, 1))?;
        drop(page);

        let search_key = Constant::string("test_key");
        let mut leaf = BTreeLeaf::new(tx.clone(), blk, layout, search_key)?;

        assert!(leaf.try_overflow()?);
        Ok(())
    }

    #[test]
    fn test_btree_leaf_try_overflow_no_match() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut schema = Schema::new();
        schema.add_string_field("dataval", 8);
        schema.add_int_field("block");
        schema.add_int_field("id");

        let layout = Layout::new(schema);
        let blk = tx.append("testindex")?;
        
        // Create a page with different key
        let page = BTPage::new(tx.clone(), blk.clone(), layout.clone())?;
        page.set_flag(42)?; // Set overflow block number
        page.insert_leaf(0, &Constant::string("diff_key"), &crate::record::RID::new(1, 1))?;
        drop(page);

        let search_key = Constant::string("test_key");
        let mut leaf = BTreeLeaf::new(tx.clone(), blk, layout, search_key)?;

        // Should not move to overflow page due to key mismatch
        assert!(!leaf.try_overflow()?);

        Ok(())
    }
}