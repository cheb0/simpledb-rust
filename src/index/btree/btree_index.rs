use crate::error::{DbError, DbResult};
use crate::record::{Layout, Schema, FieldType};
use crate::storage::BlockId;
use crate::tx::Transaction;
use crate::query::Constant;

use super::{BTPage, BTreeLeaf};

/// Represents a B+Tree index that provides efficient lookup and range queries.
/// The index consists of leaf pages (containing actual data RIDs) and
/// directory pages (containing navigation information).
pub struct BTreeIndex<'a> {
    tx: Transaction<'a>,
    dir_layout: Layout,
    leaf_layout: Layout,
    leaf_tbl: String,
    leaf: Option<BTreeLeaf<'a>>,
    root_blk: BlockId,
}

impl<'a> BTreeIndex<'a> {
    /// Opens a B-tree index for the specified index.
    /// The method determines the appropriate files
    /// for the leaf and directory records,
    /// creating them if they did not exist.
    /// 
    /// # Arguments
    /// * `tx` - The calling transaction
    /// * `idx_name` - The name of the index
    /// * `leaf_layout` - The schema of the leaf index records
    pub fn new(
        tx: Transaction<'a>,
        idx_name: &str,
        leaf_layout: Layout,
    ) -> DbResult<Self> {
        let leaf_tbl = format!("{}leaf", idx_name);
        
        if tx.size(&leaf_tbl)? == 0 {
            let blk = tx.append(&leaf_tbl)?;
            let node = BTPage::new(tx.clone(), blk.clone(), leaf_layout.clone())?;
            node.format(&blk, -1)?;
        }

        let mut dir_schema = Schema::new();
        dir_schema.add_from_schema("block", &leaf_layout.schema());
        dir_schema.add_from_schema("dataval", &leaf_layout.schema());
        
        let dir_tbl = format!("{}dir", idx_name);
        let dir_layout = Layout::new(dir_schema);
        let root_blk = BlockId::new(dir_tbl.clone(), 0);
        
        if tx.size(&dir_tbl)? == 0 {
            // Create new root block
            tx.append(&dir_tbl)?;
            let node = BTPage::new(tx.clone(), root_blk.clone(), dir_layout.clone())?;
            node.format(&root_blk, 0)?;
            
            // Insert initial directory entry
            let fld_type = dir_layout.schema().field_type("dataval")
                .ok_or_else(|| DbError::Schema("dataval field not found in directory schema".to_string()))?;
            
            let min_val = match fld_type {
                FieldType::Integer => Constant::int(i32::MIN),
                FieldType::Varchar => Constant::string(""),
            };
            
            node.insert_dir(0, &min_val, 0)?;
        }

        Ok(BTreeIndex {
            tx,
            dir_layout,
            leaf_layout,
            leaf_tbl,
            leaf: None,
            root_blk,
        })
    }

    /// Get the leaf table name
    pub fn leaf_tbl(&self) -> &str {
        &self.leaf_tbl
    }

    /// Get the directory layout
    pub fn dir_layout(&self) -> &Layout {
        &self.dir_layout
    }

    pub fn leaf_layout(&self) -> &Layout {
        &self.leaf_layout
    }

    pub fn root_blk(&self) -> &BlockId {
        &self.root_blk
    }

    pub fn leaf(&self) -> Option<&BTreeLeaf<'a>> {
        self.leaf.as_ref()
    }

    pub fn leaf_mut(&mut self) -> Option<&mut BTreeLeaf<'a>> {
        self.leaf.as_mut()
    }

    pub fn set_leaf(&mut self, leaf: BTreeLeaf<'a>) {
        self.leaf = Some(leaf);
    }

    pub fn clear_leaf(&mut self) {
        self.leaf = None;
    }
}

impl<'a> Drop for BTreeIndex<'a> {
    fn drop(&mut self) {
        // Clear leaf to ensure proper cleanup
        self.leaf = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::testing_utils::temp_db;

    #[test]
    fn test_btree_index_creation() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut leaf_schema = Schema::new();
        leaf_schema.add_string_field("dataval", 10);
        leaf_schema.add_int_field("block");
        leaf_schema.add_int_field("id");

        let leaf_layout = Layout::new(leaf_schema);
        let index = BTreeIndex::new(tx.clone(), "test_index", leaf_layout)?;

        assert!(tx.size("test_indexleaf")? > 0);
        assert!(tx.size("test_indexdir")? > 0);

        assert_eq!(index.leaf_tbl(), "test_indexleaf");
        assert!(index.dir_layout().schema().has_field("block"));
        assert!(index.dir_layout().schema().has_field("dataval"));

        Ok(())
    }

    #[test]
    fn test_btree_index_reuse_existing() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut leaf_schema = Schema::new();
        leaf_schema.add_string_field("dataval", 10);
        leaf_schema.add_int_field("block");
        leaf_schema.add_int_field("id");

        let leaf_layout = Layout::new(leaf_schema);
        
        // Create index twice - should reuse existing files
        let _index1 = BTreeIndex::new(tx.clone(), "test_index", leaf_layout.clone())?;
        let _index2 = BTreeIndex::new(tx.clone(), "test_index", leaf_layout)?;

        // Files should still exist
        assert!(tx.size("test_indexleaf")? > 0);
        assert!(tx.size("test_indexdir")? > 0);

        Ok(())
    }

    #[test]
    fn test_btree_index_with_integer_dataval() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let mut leaf_schema = Schema::new();
        leaf_schema.add_int_field("dataval");
        leaf_schema.add_int_field("block");
        leaf_schema.add_int_field("id");

        let leaf_layout = Layout::new(leaf_schema);
        let index = BTreeIndex::new(tx.clone(), "int_index", leaf_layout)?;

        // Check that files were created
        assert!(tx.size("int_indexleaf")? > 0);
        assert!(tx.size("int_indexdir")? > 0);

        Ok(())
    }
} 