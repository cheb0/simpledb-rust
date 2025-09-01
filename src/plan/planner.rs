use std::sync::Arc;

use crate::{
    DbResult,
    index::Index,
    metadata::MetadataMgr,
    parse::{Parser, Statement},
    plan::{
        Plan,
        project_plan::ProjectPlan,
        table_plan::{TablePlan, TablePlanner},
    },
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
                    let table_planner =
                        TablePlanner::new(table_name, pred, tx.clone(), &self.metadata_mgr)?;
                    plan = table_planner.make_select_plan();
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
            Statement::CreateIndex { name, table_name, column } => {
                self.execute_create_index(&name, &table_name, &column, tx)
            }
            _ => Err(crate::error::DbError::Schema(
                "Only INSERT, UPDATE, CREATE TABLE and CREATE INDEX statements are supported for updates"
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
        let mut scan: TableScan<'_> = TableScan::new(tx.clone(), table_name, layout)?;

        scan.move_to_last()?;
        scan.insert()?;
        let rid = scan.get_rid()?;
        let indexes = self.metadata_mgr.get_index_info(table_name, tx.clone())?;

        for (field, value) in fields.iter().zip(values.iter()) {
            scan.set_val(field, value.clone())?;

            if let Some(index_info) = indexes.get(field) {
                let mut index = index_info.open(tx.clone())?;
                index.insert(value, &rid)?;
                index.close();
            }
        }

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

    fn execute_create_index(
        &self,
        name: &str,
        table_name: &str,
        column: &str,
        tx: Transaction<'_>,
    ) -> DbResult<i32> {
        self.metadata_mgr
            .create_index(name, table_name, column, tx)?;
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
            scan.before_first()?;
            while scan.next()? {
                for (field, value) in fields.iter().zip(values.iter()) {
                    scan.set_val(field, value.clone())?;
                }
                affected_rows += 1;
            }
        }

        Ok(affected_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::Index,
        query::Constant,
        record::{Layout, schema::Schema},
        utils::testing_utils::temp_db,
    };

    #[test]
    fn test_execute_insert_with_index_maintenance() -> DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");

        let tx = db.new_tx()?;

        db.metadata_mgr()
            .create_table("test_table", &schema, tx.clone())?;
        db.metadata_mgr()
            .create_index("age_idx", "test_table", "age", tx.clone())?;

        let insert_sql = "INSERT INTO test_table (id, name, age) VALUES (1, 'Alice', 25)";
        let result = db.planner().execute_update(insert_sql, tx.clone())?;
        assert_eq!(result, 1);

        let indexes = db.metadata_mgr().get_index_info("test_table", tx.clone())?;
        assert!(indexes.contains_key("age"));

        let age_index_info = indexes.get("age").unwrap();
        let mut age_index = age_index_info.open(tx.clone())?;

        age_index.before_first(&Constant::int(25))?;
        assert!(age_index.next()?, "Should find the inserted age value");

        let rid = age_index.get_data_rid()?;
        assert_eq!(rid.block_number(), 0);
        assert_eq!(rid.slot(), 1);
        age_index.close();

        // Verify we can actually navigate to record
        {
            let mut table_scan = TableScan::new(tx.clone(), "test_table", Layout::new(schema))?;
            table_scan.move_to_rid(rid)?;
            assert_eq!(1, table_scan.get_int("id")?);
            assert_eq!("Alice", table_scan.get_string("name")?);
            assert_eq!(25, table_scan.get_int("age")?);
        }

        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_execute_insert_with_string_index() -> DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");

        let tx = db.new_tx()?;

        db.metadata_mgr()
            .create_table("test_table", &schema, tx.clone())?;
        db.metadata_mgr()
            .create_index("name_idx", "test_table", "name", tx.clone())?;

        let result = db.planner().execute_update(
            &format!("INSERT INTO test_table (id, name, age) VALUES (1, 'Bob', 30)"),
            tx.clone(),
        )?;
        assert_eq!(result, 1);

        let indexes = db.metadata_mgr().get_index_info("test_table", tx.clone())?;
        assert!(indexes.contains_key("name"));

        let name_index_info = indexes.get("name").unwrap();
        let mut name_index = name_index_info.open(tx.clone())?;

        name_index.before_first(&Constant::string("Bob"))?;
        assert!(name_index.next()?, "Should find the inserted name value");

        let rid = name_index.get_data_rid()?;
        assert_eq!(rid.block_number(), 0);
        assert_eq!(rid.slot(), 1);

        name_index.close();
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_execute_insert_without_indexes() -> DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");

        let tx = db.new_tx()?;
        db.metadata_mgr()
            .create_table("test_table", &schema, tx.clone())?;

        let result = db.planner().execute_update(
            &format!("INSERT INTO test_table (id, name, age) VALUES (1, 'Charlie', 35)"),
            tx.clone(),
        )?;
        assert_eq!(result, 1);

        let indexes = db.metadata_mgr().get_index_info("test_table", tx.clone())?;
        assert_eq!(indexes.len(), 0);

        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_execute_create_index() -> DbResult<()> {
        let db = temp_db()?;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");

        let tx = db.new_tx()?;

        db.metadata_mgr()
            .create_table("test_table", &schema, tx.clone())?;

        let result = db
            .planner()
            .execute_update("CREATE INDEX age_idx ON test_table (age)", tx.clone())?;
        assert_eq!(result, 1);

        let indexes = db.metadata_mgr().get_index_info("test_table", tx.clone())?;
        assert!(indexes.contains_key("age"));

        let insert_sql = "INSERT INTO test_table (id, name, age) VALUES (1, 'Alice', 25)";
        let result = db.planner().execute_update(insert_sql, tx.clone())?;
        assert_eq!(result, 1);

        let age_index_info = indexes.get("age").unwrap();
        let mut age_index = age_index_info.open(tx.clone())?;

        age_index.before_first(&Constant::int(25))?;
        assert!(age_index.next()?, "Should find the inserted age value");

        let rid = age_index.get_data_rid()?;
        assert_eq!(rid.block_number(), 0);
        assert_eq!(rid.slot(), 1);
        age_index.close();

        tx.commit()?;
        Ok(())
    }
}
