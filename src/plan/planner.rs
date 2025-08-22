use std::sync::Arc;

use crate::{
    DbResult,
    metadata::MetadataMgr,
    parse::{Parser, Statement},
    plan::{Plan, project_plan::ProjectPlan, select_plan::SelectPlan, table_plan::TablePlan},
    query::{Scan, UpdateScan},
    record::{Schema, TableScan},
    tx::Transaction,
};

pub struct Planner {
    parser: Parser,
    metadata_mgr: Arc<MetadataMgr>,
}

impl Planner {
    pub fn new(metadata_mgr: Arc<MetadataMgr>) -> Self {
        Planner {
            parser: Parser::new(),
            metadata_mgr,
        }
    }

    pub fn create_query_plan(&self, query: &str, tx: Transaction<'_>) -> DbResult<Box<dyn Plan>> {
        let stmt = self.parser.parse(query)?;

        match stmt {
            Statement::Query {
                fields,
                tables,
                predicate,
            } => {
                if tables.len() != 1 {
                    return Err(crate::error::DbError::Schema(
                        "Only single table queries are supported".to_string(),
                    ));
                }

                let table_name = &tables[0];
                let layout = self.metadata_mgr.get_layout(table_name, tx.clone())?;
                let table_plan = TablePlan::new(table_name.to_string(), layout)?;
                let mut plan: Box<dyn Plan> = Box::new(table_plan);

                if let Some(pred) = predicate {
                    plan = Box::new(SelectPlan::new(plan, pred));
                }

                if !(fields.len() == 1 && fields[0] == "*") {
                    plan = Box::new(ProjectPlan::new(plan, fields));
                }

                Ok(plan)
            }
            _ => Err(crate::error::DbError::Schema(
                "Only SELECT statements are supported for queries".to_string(),
            )),
        }
    }

    pub fn execute_update(&self, cmd: &str, tx: Transaction<'_>) -> DbResult<i32> {
        let stmt = self.parser.parse(cmd)?;

        match stmt {
            Statement::Insert {
                table_name,
                fields,
                values,
            } => self.execute_insert(&table_name, &fields, &values, tx),
            Statement::Update {
                table_name,
                fields,
                values,
                predicate,
            } => self.execute_update_statement(&table_name, &fields, &values, predicate, tx),
            Statement::CreateTable { table_name, schema } => {
                self.execute_create_table(&table_name, &schema, tx)
            }
            _ => Err(crate::error::DbError::Schema(
                "Only INSERT, UPDATE and CREATE TABLE statements are supported for updates"
                    .to_string(),
            )),
        }
    }

    fn execute_insert(
        &self,
        table_name: &str,
        fields: &[String],
        values: &[crate::query::Constant],
        tx: Transaction<'_>,
    ) -> DbResult<i32> {
        let layout = self.metadata_mgr.get_layout(table_name, tx.clone())?;
        let mut scan = TableScan::new(tx.clone(), table_name, layout)?;

        scan.insert()?;
        for (field, value) in fields.iter().zip(values.iter()) {
            scan.set_val(field, value.clone())?;
        }
        scan.close();

        Ok(1)
    }

    fn execute_create_table(
        &self,
        table_name: &str,
        schema: &Schema,
        tx: Transaction<'_>,
    ) -> DbResult<i32> {
        self.metadata_mgr.create_table(table_name, schema, tx)?;
        Ok(1)
    }

    fn execute_update_statement(
        &self,
        table_name: &str,
        fields: &[String],
        values: &[crate::query::Constant],
        predicate: Option<crate::query::Predicate>,
        tx: Transaction<'_>,
    ) -> DbResult<i32> {
        let layout = self.metadata_mgr.get_layout(table_name, tx.clone())?;
        let mut scan = TableScan::new(tx.clone(), table_name, layout)?;

        let mut affected_rows = 0;

        // TODO use index to find by predicate
        if let Some(pred) = predicate {
            scan.before_first()?;
            while scan.next()? {
                if pred.is_satisfied(&mut scan)? {
                    for (field, value) in fields.iter().zip(values.iter()) {
                        scan.set_val(field, value.clone())?;
                    }
                    affected_rows += 1;
                }
            }
        } else {
            // Update all records
            scan.before_first()?;
            while scan.next()? {
                for (field, value) in fields.iter().zip(values.iter()) {
                    scan.set_val(field, value.clone())?;
                }
                affected_rows += 1;
            }
        }

        scan.close();
        Ok(affected_rows)
    }
}
