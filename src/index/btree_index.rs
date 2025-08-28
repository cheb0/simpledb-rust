use crate::{
    DbResult,
    index::{
        BTreePage, Index, btree_internal::BTreeInternal, btree_leaf::BTreeLeaf,
        btree_page::PageType,
    },
    metadata::IndexInfo,
    query::Constant,
    record::{Layout, RID, Schema, schema::FieldType},
    storage::BlockId,
    tx::Transaction,
};
use std::fmt;

// Original implementation - https://github.com/redixhumayun/simpledb/blob/master/src/btree.rs

/// A B-tree implementation of the Index interface/
pub struct BTreeIndex<'tx> {
    tx: Transaction<'tx>,
    index_name: String,
    internal_layout: Layout,
    leaf_layout: Layout,
    leaf_table_name: String,
    leaf: Option<BTreeLeaf<'tx>>,
    root_block: BlockId,
}

impl<'tx> BTreeIndex<'tx> {
    pub fn new(tx: Transaction<'tx>, index_name: &str, leaf_layout: Layout) -> DbResult<Self> {
        let leaf_table_name = format!("{}leaf", index_name);
        if tx.size(&leaf_table_name)? == 0 {
            let block_id = tx.append(&leaf_table_name)?;
            let btree_page = BTreePage::new(tx.clone(), block_id, leaf_layout.clone())?;
            btree_page.format(PageType::Leaf(None))?;
        }

        //  Create the internal file with the schema required if it does not exist
        let internal_table_name = format!("{}internal", index_name);
        let mut internal_schema = Schema::new();
        internal_schema.add_from_schema(IndexInfo::BLOCK_NUM_FIELD, leaf_layout.schema());
        internal_schema.add_from_schema(IndexInfo::DATA_FIELD, leaf_layout.schema());
        let internal_layout = Layout::new(internal_schema.clone());

        if tx.size(&internal_table_name)? == 0 {
            let block_id = tx.append(&internal_table_name)?;
            let internal_page = BTreePage::new(tx.clone(), block_id, internal_layout.clone())?;
            internal_page.format(PageType::Internal(None))?;

            //  insert initial entry
            let field_type = internal_schema.field_type(IndexInfo::DATA_FIELD).unwrap();
            let min_val = match field_type {
                FieldType::Integer => Constant::Int(i32::MIN),
                FieldType::Varchar => Constant::String("".to_string()),
            };
            internal_page.insert_internal(0, min_val, 0)?;
        }
        Ok(Self {
            tx,
            index_name: index_name.to_string(),
            internal_layout,
            leaf_layout,
            leaf_table_name,
            leaf: None,
            root_block: BlockId::new(internal_table_name, 0),
        })
    }
}

impl<'tx> fmt::Display for BTreeIndex<'tx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n=== BTreeIndex: {} ===", self.index_name)?;
        writeln!(f, "Root block: {:?}", self.root_block)?;
        writeln!(f, "Leaf table: {}", self.leaf_table_name)?;

        let tx = self.tx.clone();
        let internal_cnt = tx.size(&self.root_block.file_name()).unwrap();

        for i in 0..internal_cnt {
            let page = BTreeInternal::new(
                tx.clone(),
                BlockId::new(self.root_block.file_name().to_string(), i),
                self.internal_layout.clone(),
                self.root_block.file_name().to_string(),
            )
            .unwrap();
            writeln!(f, "{}", page)?;
        }

        let leaf_cnt = tx.size(&self.leaf_table_name).unwrap();

        for i in 0..leaf_cnt {
            let page = BTreeLeaf::new(
                tx.clone(),
                BlockId::new(self.leaf_table_name.clone(), i),
                self.leaf_layout.clone(),
                Constant::Int(-1),
                self.leaf_table_name.to_string(),
            )
            .unwrap();
            writeln!(f, "{}", page)?;
        }

        Ok(())
    }
}

impl<'tx> Index for BTreeIndex<'tx> {
    fn before_first(&mut self, search_key: &Constant) -> DbResult<()> {
        self.close();
        let mut root = BTreeInternal::new(
            self.tx.clone(),
            self.root_block.clone(),
            self.internal_layout.clone(),
            self.root_block.file_name().to_string(),
        )?;
        let leaf_block_num = root.search(search_key)?;
        let leaf_block_id = BlockId::new(self.leaf_table_name.clone(), leaf_block_num as i32);
        self.leaf = Some(
            BTreeLeaf::new(
                self.tx.clone(),
                leaf_block_id.clone(),
                self.leaf_layout.clone(),
                search_key.clone(),
                leaf_block_id.file_name().to_string(),
            )
            .unwrap(),
        );
        Ok(())
    }

    fn next(&mut self) -> DbResult<bool> {
        match self
            .leaf
            .as_mut()
            .expect("Leaf not initialized, did you forget to call before_first?")
            .next()?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    fn get_data_rid(&self) -> DbResult<RID> {
        self.leaf.as_ref().unwrap().get_data_rid()
    }

    fn insert(&mut self, data_val: &Constant, data_rid: &RID) -> DbResult<()> {
        self.before_first(data_val)?;
        let int_node_id = self.leaf.as_mut().unwrap().insert(*data_rid).unwrap();
        if int_node_id.is_none() {
            return Ok(());
        }

        let int_node_id = int_node_id.unwrap();
        let root = BTreeInternal::new(
            self.tx.clone(),
            self.root_block.clone(),
            self.internal_layout.clone(),
            self.root_block.file_name().to_string(),
        )?;
        let root_split_entry = root.insert(int_node_id)?;
        if root_split_entry.is_none() {
            return Ok(());
        }
        let root_split_entry = root_split_entry.unwrap();
        root.make_new_root(root_split_entry)
    }

    fn delete(&mut self, data_val: &Constant, data_rid: &RID) -> DbResult<()> {
        self.before_first(data_val)?;
        self.leaf.as_mut().unwrap().delete(*data_rid)?;
        //  TODO: Should the leaf be set to None here?
        self.leaf = None;
        Ok(())
    }

    fn close(&mut self) {
        if self.leaf.is_some() {
            self.leaf = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SimpleDB,
        utils::testing_utils::{temp_db, temp_db_with_cfg},
    };
    use rand::{Rng, seq::SliceRandom};
    use std::collections::{HashMap, HashSet};

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("dataval");
        schema.add_int_field("block");
        schema.add_int_field("id");
        Layout::new(schema)
    }

    fn setup_index<'tx>(db: &'tx SimpleDB) -> DbResult<BTreeIndex<'tx>> {
        let tx = db.new_tx()?;
        let layout = create_test_layout();
        let index_name = "test";
        BTreeIndex::new(tx.clone(), &index_name, layout)
    }

    #[test]
    fn test_btree_index_construction() -> DbResult<()> {
        let db = temp_db()?;
        let index = setup_index(&db)?;

        // Verify internal node file exists with minimum value entry
        let root = BTreeInternal::new(
            index.tx.clone(),
            index.root_block.clone(),
            index.internal_layout.clone(),
            index.root_block.file_name().to_string(),
        )?;
        assert_eq!(root.contents.get_number_of_recs()?, 1);
        assert_eq!(root.contents.get_data_value(0)?, Constant::Int(i32::MIN));
        Ok(())
    }

    #[test]
    fn test_simple_insert_and_search() -> DbResult<()> {
        let db = temp_db_with_cfg(|cfg| cfg.block_size(400))?;
        let mut index = setup_index(&db)?;

        index.insert(&Constant::Int(0), &RID::new(1, 1))?;
        index.insert(&Constant::Int(1), &RID::new(1, 2))?;
        index.insert(&Constant::Int(2), &RID::new(1, 3))?;

        index.before_first(&Constant::Int(0))?;
        assert!(index.next()?);
        assert_eq!(index.get_data_rid()?, RID::new(1, 1));

        index.before_first(&Constant::Int(1))?;
        assert!(index.next()?);
        assert_eq!(index.get_data_rid()?, RID::new(1, 2));

        index.before_first(&Constant::Int(2))?;
        assert!(index.next()?);
        assert_eq!(index.get_data_rid()?, RID::new(1, 3));
        Ok(())
    }

    #[test]
    fn test_duplicate_keys() -> DbResult<()> {
        let db = temp_db()?;
        let mut index = setup_index(&db)?;

        index.insert(&Constant::Int(10), &RID::new(1, 1))?;
        index.insert(&Constant::Int(10), &RID::new(1, 2))?;
        index.insert(&Constant::Int(10), &RID::new(1, 3))?;

        // Search and verify all duplicates are found
        index.before_first(&Constant::Int(10))?;

        let mut found_rids = Vec::new();
        while index.next()? {
            found_rids.push(index.get_data_rid()?);
        }

        assert_eq!(found_rids.len(), 3);
        assert!(found_rids.contains(&RID::new(1, 1)));
        assert!(found_rids.contains(&RID::new(1, 2)));
        assert!(found_rids.contains(&RID::new(1, 3)));
        Ok(())
    }

    #[test]
    fn test_delete() -> DbResult<()> {
        let db = temp_db()?;
        let mut index = setup_index(&db)?;

        index.insert(&Constant::Int(10), &RID::new(1, 1))?;
        index.delete(&Constant::Int(10), &RID::new(1, 1))?;

        // Verify value is gone
        index.before_first(&Constant::Int(10))?;
        assert!(!index.next()?);

        index.insert(&Constant::Int(20), &RID::new(1, 1))?;
        index.insert(&Constant::Int(20), &RID::new(1, 2))?;
        index.delete(&Constant::Int(20), &RID::new(1, 1))?;

        index.before_first(&Constant::Int(20))?;
        assert!(index.next()?);
        assert_eq!(index.get_data_rid()?, RID::new(1, 2));
        assert!(!index.next()?);
        Ok(())
    }

    #[test]
    fn test_btree_split() -> DbResult<()> {
        let db = temp_db()?;
        let mut index = setup_index(&db)?;

        // Insert enough values to force splits
        for i in 0..24 {
            index.insert(&Constant::Int(i), &RID::new(1, i as usize))?;
        }

        // Verify we can still find values after splits
        for i in 0..24 {
            index.before_first(&Constant::Int(i))?;
            assert!(index.next()?);
            assert_eq!(index.get_data_rid()?, RID::new(1, i as usize));
        }
        Ok(())
    }

    #[test]
    fn test_index_10k_randomized_keys() -> DbResult<()> {
        let db = temp_db_with_cfg(|cfg| cfg.block_size(400))?;
        let mut index = setup_index(&db)?;

        const NUM_KEYS: usize = 10000;
        let mut keys: Vec<i32> = (0..NUM_KEYS as i32).collect();
        let mut rng = rand::rng();
        keys.shuffle(&mut rng);

        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new((i / 100) as i32, i % 100);
            index.insert(&Constant::Int(key), &rid)?;
        }

        let mut missing_keys = Vec::new();

        for (i, &key) in keys.iter().enumerate() {
            index.before_first(&Constant::Int(key))?;

            if index.next()? {
                let rid = index.get_data_rid()?;

                let expected_rid = RID::new((i / 100) as i32, i % 100);
                assert_eq!(rid, expected_rid, "RID mismatch for key {}", key);
            } else {
                missing_keys.push(key);
            }
        }

        if !missing_keys.is_empty() {
            println!(
                "Missing keys: {:?}",
                &missing_keys[..std::cmp::min(50, missing_keys.len())]
            );
            if missing_keys.len() > 50 {
                println!("... and {} more", missing_keys.len() - 50);
            }
        }
        assert!(missing_keys.is_empty(), "No keys should be missing");

        let non_existent_keys = vec![
            -1,
            -100,
            -1000,
            NUM_KEYS as i32,
            (NUM_KEYS + 100) as i32,
            (NUM_KEYS + 1000) as i32,
        ];
        for &key in &non_existent_keys {
            index.before_first(&Constant::Int(key))?;
            assert!(!index.next()?, "Key {} should not exist in the index", key);
        }

        Ok(())
    }

    #[test]
    fn test_index_10k_randomized_keys_with_delete() -> DbResult<()> {
        let db = temp_db_with_cfg(|cfg| cfg.block_size(400))?;
        let mut index = setup_index(&db)?;

        const NUM_KEYS: usize = 10000;
        let mut keys: Vec<i32> = (0..NUM_KEYS as i32).collect();
        let mut keys_to_delete = HashSet::new();
        let mut key_to_rid = HashMap::new();
        let mut rng = rand::rng();
        keys.shuffle(&mut rng);

        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new((i / 100) as i32, i % 100);
            index.insert(&Constant::Int(key), &rid)?;
            key_to_rid.insert(key, rid);

            // delete 10% of keys
            if rng.random_range(0..10) == 0 {
                keys_to_delete.insert(key);
            }
        }

        for (i, &key) in keys.iter().enumerate() {
            index.before_first(&Constant::Int(key))?;

            if index.next()? {
                let rid = index.get_data_rid()?;

                let expected_rid = RID::new((i / 100) as i32, i % 100);
                assert_eq!(rid, expected_rid, "RID mismatch for key {}", key);
            } else {
                assert!(false, "Key {} not found", key);
            }
        }

        for &key in keys_to_delete.iter() {
            let rid = key_to_rid[&key];
            index.delete(&Constant::Int(key), &rid).unwrap();
        }

        for (i, &key) in keys.iter().enumerate() {
            index.before_first(&Constant::Int(key))?;

            if keys_to_delete.contains(&key) {
                assert!(!index.next().unwrap(), "Should never find a deleted entry");
            } else {
                if index.next()? {
                    let rid = index.get_data_rid()?;
                    let expected_rid = RID::new((i / 100) as i32, i % 100);
                    assert_eq!(rid, expected_rid, "RID mismatch for key {}", key);
                } else {
                    assert!(false, "Key {} not found", key);
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_index_sequential_access() -> DbResult<()> {
        let db = temp_db_with_cfg(|cfg| cfg.block_size(400))?;
        let mut index = setup_index(&db)?;

        const NUM_KEYS: usize = 10000;
        for i in 0..NUM_KEYS {
            let key = i as i32;
            let rid = RID::new((i / 100) as i32, i % 100);
            index.insert(&Constant::Int(key), &rid)?;
        }

        let mut found_count = 0;
        for i in 0..NUM_KEYS {
            let key = i as i32;
            index.before_first(&Constant::Int(key))?;

            if index.next()? {
                let rid = index.get_data_rid()?;
                let expected_rid = RID::new((i / 100) as i32, i % 100);
                assert_eq!(rid, expected_rid, "RID mismatch for sequential key {}", key);
                found_count += 1;
            }
        }

        assert_eq!(found_count, NUM_KEYS, "All sequential keys should be found");
        Ok(())
    }
}
