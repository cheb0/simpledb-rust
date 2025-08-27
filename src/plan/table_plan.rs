use super::Plan;
use crate::DbResult;
use crate::metadata::{IndexInfo, MetadataMgr};
use crate::plan::IndexSelectPlan;
use crate::plan::select_plan::SelectPlan;
use crate::query::Predicate;
use crate::query::Scan;
use crate::record::TableScan;
use crate::record::layout::Layout;
use crate::record::schema::Schema;
use crate::tx::transaction::Transaction;
use std::collections::HashMap;

#[derive(Clone)]
pub struct TablePlan {
    tblname: String,
    layout: Layout,
}

impl TablePlan {
    pub fn new(tblname: String, layout: Layout) -> DbResult<Self> {
        Ok(TablePlan {
            tblname: tblname,
            layout,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.tblname
    }

    pub fn table_layout(&self) -> &Layout {
        &self.layout
    }
}

impl Plan for TablePlan {
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx> {
        Box::new(TableScan::new(tx, &self.tblname, self.layout.clone()).unwrap())
    }

    fn schema(&self) -> Schema {
        self.layout.schema().clone()
    }
}

/// This struct contains methods for planning a single table.
/// It determines the most efficient way to execute a query on a table,
/// including when to use indexes for better performance.
pub struct TablePlanner<'tx> {
    plan: TablePlan,
    pred: Predicate,
    schema: Schema,
    indexes: HashMap<String, IndexInfo>,
    tx: Transaction<'tx>,
}

impl<'tx> TablePlanner<'tx> {
    pub fn new(
        tblname: &str,
        pred: Predicate,
        tx: Transaction<'tx>,
        mdm: &MetadataMgr,
    ) -> DbResult<Self> {
        let layout = mdm.get_layout(tblname, tx.clone())?;
        let plan = TablePlan::new(tblname.to_string(), layout.clone())?;
        let schema = plan.schema();
        let indexes = mdm.get_index_info(tblname, tx.clone())?;

        Ok(TablePlanner {
            plan,
            pred,
            schema,
            indexes,
            tx: tx.clone(),
        })
    }

    /// Creates a select plan for the table, using indexes when possible for better performance.
    pub fn make_select_plan(&self) -> Box<dyn Plan> {
        let mut plan = self.try_index_select();
        if plan.is_none() {
            plan = Some(Box::new(self.plan.clone()) as Box<dyn Plan>);
        }
        self.add_select_pred(plan.unwrap())
    }

    /// Attempts to create an index-based select plan if the predicate can use an index.
    /// Returns None if no suitable index is found.
    fn try_index_select(&self) -> Option<Box<dyn Plan>> {
        for (fldname, index) in &self.indexes {
            if let Some(val) = self.pred.equates_with_constant(fldname) {
                return Some(Box::new(IndexSelectPlan::new(
                    self.plan.clone(),
                    index.clone(),
                    val.clone(),
                )));
            }
        }
        None
    }

    /// Adds a select predicate to the given plan if the predicate applies to the table schema.
    fn add_select_pred(&self, plan: Box<dyn Plan>) -> Box<dyn Plan> {
        if let Some(select_pred) = self.pred.select_sub_pred(&self.schema) {
            return Box::new(SelectPlan::new(plan, select_pred));
        }
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Constant, Expr, Term};
    use crate::utils::testing_utils::temp_db;

    #[test]
    fn test_table_planner_make_select_plan() -> DbResult<()> {
        let db = temp_db()?;

        // Create a table
        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");

        let tx = db.new_tx()?;
        db.metadata_mgr()
            .create_table("test_table", &schema, tx.clone())?;

        let term = Term::new(Expr::field_name("age"), Expr::constant(Constant::int(25)));
        let predicate = Predicate::new(term);

        let planner = TablePlanner::new("test_table", predicate, tx.clone(), db.metadata_mgr())?;

        let plan = planner.make_select_plan();

        assert_eq!(plan.schema().fields().len(), 3);
        assert!(plan.schema().has_field("id"));
        assert!(plan.schema().has_field("name"));
        assert!(plan.schema().has_field("age"));

        tx.commit()?;
        Ok(())
    }
}
