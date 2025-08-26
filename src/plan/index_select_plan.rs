use crate::index::BTreeIndex;
use crate::metadata::IndexInfo;
use crate::plan::Plan;
use crate::query::Constant;
use crate::query::{IndexSelectScan, Scan};
use crate::record::{TableScan, layout::Layout, schema::Schema};
use crate::tx::Transaction;

pub struct IndexSelectPlan {
    plan: Box<dyn Plan>,
    index_name: String,
    field_name: String,
    table_name: String,
    search_value: Constant,
}

impl IndexSelectPlan {
    pub fn new(
        plan: Box<dyn Plan>,
        index_name: String,
        field_name: String,
        table_name: String,
        search_value: Constant,
    ) -> Self {
        IndexSelectPlan {
            plan,
            index_name,
            field_name,
            table_name,
            search_value,
        }
    }
}

impl Plan for IndexSelectPlan {
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx> {
        let index_layout = IndexInfo::create_idx_layout(&self.field_name, &self.plan.schema());
        let index = BTreeIndex::new(tx.clone(), &self.index_name, index_layout).unwrap();

        let table_scan = TableScan::new(
            tx,
            &self.table_name,
            Layout::new(self.plan.schema().clone()),
        )
        .unwrap();

        Box::new(
            IndexSelectScan::new(table_scan, Box::new(index), self.search_value.clone()).unwrap(),
        )
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        record::{layout::Layout, schema::Schema},
        utils::testing_utils::temp_db,
    };

    #[test]
    fn test_index_select_plan_basic() -> crate::DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");
        let layout = Layout::new(schema.clone());

        let tx = db.new_tx()?;

        let table_plan = crate::plan::TablePlan::new("test_table".to_string(), layout.clone())?;

        let index_select_plan = IndexSelectPlan::new(
            Box::new(table_plan),
            "age_idx".to_string(),
            "age".to_string(),
            "test_table".to_string(),
            Constant::int(25),
        );

        assert_eq!(index_select_plan.schema().fields().len(), 3);
        assert!(index_select_plan.schema().has_field("id"));
        assert!(index_select_plan.schema().has_field("name"));
        assert!(index_select_plan.schema().has_field("age"));

        let scan = index_select_plan.open(tx.clone());

        assert!(scan.has_field("id"));
        assert!(scan.has_field("name"));
        assert!(scan.has_field("age"));

        tx.commit()?;
        Ok(())
    }
}
