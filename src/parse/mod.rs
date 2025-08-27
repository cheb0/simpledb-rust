use sqlparser::ast::{CharacterLength, DataType, SetExpr, Statement as SqlStatement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::error::{DbError, DbResult};
use crate::query::predicate::Predicate;
use crate::query::{Constant, Expr, Term};
use crate::record::schema::Schema;

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable {
        table_name: String,
        schema: Schema,
    },
    CreateIndex {
        name: String,
        table_name: String,
        column: String, // only a single column is supported for index now
    },
    Insert {
        table_name: String,
        fields: Vec<String>,
        values: Vec<Constant>,
    },
    Update {
        table_name: String,
        fields: Vec<String>,
        values: Vec<Constant>,
        predicate: Option<Predicate>,
    },
    Query {
        fields: Vec<String>,
        tables: Vec<String>,
        predicate: Option<Predicate>,
    },
}

pub struct Parser {
    dialect: GenericDialect,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            dialect: GenericDialect::default(),
        }
    }

    pub fn parse(&self, sql: &str) -> DbResult<Statement> {
        let ast = SqlParser::parse_sql(&self.dialect, sql)
            .map_err(|e| DbError::Schema(format!("Failed to parse SQL: {}", e)))?;

        if ast.is_empty() {
            return Err(DbError::Schema("Empty SQL statement".to_string()));
        }

        match &ast[0] {
            SqlStatement::CreateTable(create_table) => self.parse_create_table(create_table),
            SqlStatement::CreateIndex(create_index) => self.parse_create_index(create_index),
            SqlStatement::Insert(insert) => self.parse_insert(insert),
            SqlStatement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table_name = match &table.relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
                    _ => {
                        return Err(DbError::Schema(
                            "Only simple table references are supported in UPDATE".to_string(),
                        ));
                    }
                };
                self.parse_update(&table_name, assignments, selection)
            }
            SqlStatement::Query(query) => self.parse_select(&query.body),
            _ => Err(DbError::Schema("Unsupported SQL statement".to_string())),
        }
    }

    fn parse_create_table(
        &self,
        create_table: &sqlparser::ast::CreateTable,
    ) -> DbResult<Statement> {
        let table_name = create_table.name.to_string();
        let mut schema = Schema::new();

        for col in &create_table.columns {
            let field_name = col.name.to_string();
            match col.data_type {
                DataType::Int(_) => {
                    schema.add_int_field(&field_name);
                }
                DataType::Varchar(Some(CharacterLength::IntegerLength { length, .. })) => {
                    schema.add_string_field(&field_name, length as usize);
                }
                DataType::Varchar(Some(CharacterLength::Max)) => {
                    return Err(DbError::Schema(format!(
                        "VARCHAR(MAX) is not supported for column {}",
                        field_name
                    )));
                }
                _ => {
                    return Err(DbError::Schema(format!(
                        "Unsupported data type for column {}",
                        field_name
                    )));
                }
            }
        }

        Ok(Statement::CreateTable { table_name, schema })
    }

    fn parse_create_index(
        &self,
        create_index: &sqlparser::ast::CreateIndex,
    ) -> DbResult<Statement> {
        let index_name = create_index
            .name
            .as_ref()
            .ok_or_else(|| DbError::Schema("Index name is required".to_string()))?
            .to_string();

        let table_name = create_index.table_name.to_string();

        if create_index.columns.is_empty() {
            return Err(DbError::Schema(
                "No columns specified for index".to_string(),
            ));
        }
        if create_index.columns.len() > 1 {
            return Err(DbError::Schema(
                "Only single-column indexes are supported".to_string(),
            ));
        }

        let column = create_index.columns[0].to_string();

        Ok(Statement::CreateIndex {
            name: index_name,
            table_name,
            column,
        })
    }

    fn parse_insert(&self, insert: &sqlparser::ast::Insert) -> DbResult<Statement> {
        let table_name = insert.table.to_string();

        if insert.columns.is_empty() {
            return Err(DbError::Schema("No columns provided".to_string()));
        }
        let fields = insert
            .columns
            .iter()
            .map(|col| col.value.clone())
            .collect::<Vec<String>>();
        let source = insert.source.as_ref();

        let values = match source {
            Some(query) => match &*query.body {
                SetExpr::Values(values) => {
                    if values.rows.is_empty() {
                        return Err(DbError::Schema("No values provided for INSERT".to_string()));
                    }

                    let row = &values.rows[0];
                    row.iter()
                        .map(|expr| match expr {
                            sqlparser::ast::Expr::Value(value) => match &value.value {
                                Value::SingleQuotedString(s) => Ok(Constant::String(s.clone())),
                                Value::Number(n, _) => {
                                    Ok(Constant::Int(n.parse().map_err(|_| {
                                        DbError::Schema(format!("Invalid integer value: {}", n))
                                    })?))
                                }
                                _ => Err(DbError::Schema(format!("Unsupported value type"))),
                            },
                            _ => Err(DbError::Schema(format!(
                                "Unsupported value type in INSERT statement"
                            ))),
                        })
                        .collect::<DbResult<Vec<Constant>>>()?
                }
                _ => {
                    return Err(DbError::Schema(
                        "Only VALUES clause is supported for INSERT".to_string(),
                    ));
                }
            },
            _ => {
                return Err(DbError::Schema(
                    "Only VALUES clause is supported for INSERT".to_string(),
                ));
            }
        };

        Ok(Statement::Insert {
            table_name,
            fields,
            values,
        })
    }

    fn parse_update(
        &self,
        table_name: &str,
        assignments: &[sqlparser::ast::Assignment],
        selection: &Option<sqlparser::ast::Expr>,
    ) -> DbResult<Statement> {
        let mut fields = Vec::new();
        let mut values = Vec::new();

        for assignment in assignments {
            let field_name = assignment.target.to_string();
            fields.push(field_name);

            let value =
                match &assignment.value {
                    sqlparser::ast::Expr::Value(value) => match &value.value {
                        Value::SingleQuotedString(s) => Ok(Constant::String(s.clone())),
                        Value::Number(n, _) => Ok(Constant::Int(n.parse().map_err(|_| {
                            DbError::Schema(format!("Invalid integer value: {}", n))
                        })?)),
                        _ => Err(DbError::Schema(
                            "Unsupported value type in UPDATE".to_string(),
                        )),
                    },
                    _ => Err(DbError::Schema(
                        "Only simple values are supported in UPDATE".to_string(),
                    )),
                }?;

            values.push(value);
        }

        let predicate = if let Some(where_clause) = selection {
            Some(self.parse_where_clause(where_clause)?)
        } else {
            None
        };

        Ok(Statement::Update {
            table_name: table_name.to_string(),
            fields,
            values,
            predicate,
        })
    }

    fn parse_select(&self, query: &SetExpr) -> DbResult<Statement> {
        return match query {
            SetExpr::Select(select) => {
                let fields = select
                    .projection
                    .iter()
                    .map(|item| match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(
                            sqlparser::ast::Expr::Identifier(ident),
                        ) => Ok(ident.value.clone()),
                        sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => match expr {
                            sqlparser::ast::Expr::Identifier(_ident) => Ok(alias.value.clone()),
                            _ => Err(DbError::Schema(
                                "Only simple column references are supported".to_string(),
                            )),
                        },
                        _ => Err(DbError::Schema(
                            "Only simple column references are supported".to_string(),
                        )),
                    })
                    .collect::<DbResult<Vec<String>>>()?;

                let tables = select
                    .from
                    .iter()
                    .map(|table_with_join| match &table_with_join.relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name.to_string()),
                        _ => Err(DbError::Schema(
                            "Only simple table references are supported".to_string(),
                        )),
                    })
                    .collect::<DbResult<Vec<String>>>()?;

                let predicate = if let Some(where_clause) = &select.selection {
                    Some(self.parse_where_clause(where_clause)?)
                } else {
                    None
                };

                Ok(Statement::Query {
                    fields,
                    tables,
                    predicate,
                })
            }
            _ => {
                return Err(DbError::Schema(
                    "Only VALUES clause is supported for INSERT".to_string(),
                ));
            }
        };
    }

    fn parse_where_clause(&self, expr: &sqlparser::ast::Expr) -> DbResult<Predicate> {
        match expr {
            sqlparser::ast::Expr::BinaryOp { left, op, right } => match op {
                sqlparser::ast::BinaryOperator::Eq => {
                    let field = match &**left {
                        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                        _ => {
                            return Err(DbError::Schema(
                                "Left side of = must be a field name".to_string(),
                            ));
                        }
                    };
                    let value = match &**right {
                        sqlparser::ast::Expr::Value(value) => match &value.value {
                            Value::SingleQuotedString(s) => Constant::String(s.clone()),
                            Value::Number(n, _) => Constant::Int(n.parse().map_err(|_| {
                                DbError::Schema(format!("Invalid integer value: {}", n))
                            })?),
                            _ => {
                                return Err(DbError::Schema(
                                    "Unsupported value type in WHERE clause".to_string(),
                                ));
                            }
                        },
                        _ => {
                            return Err(DbError::Schema(
                                "Right side of = must be a value".to_string(),
                            ));
                        }
                    };
                    Ok(Predicate::default()
                        .with_term(Term::new(Expr::field_name(field), Expr::constant(value))))
                }
                sqlparser::ast::BinaryOperator::And => {
                    let left_pred = self.parse_where_clause(left)?;
                    let right_pred = self.parse_where_clause(right)?;
                    Ok(left_pred.conjoin_with(right_pred))
                }
                _ => Err(DbError::Schema(
                    "Only = and AND operators are supported in WHERE clause".to_string(),
                )),
            },
            _ => Err(DbError::Schema(
                "Unsupported expression in WHERE clause".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::record::schema::FieldType;

    use super::*;

    #[test]
    fn test_parse_create_table() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "CREATE TABLE test_table (id INT, name VARCHAR(20))";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::CreateTable { table_name, schema } => {
                assert_eq!(table_name, "test_table");
                assert!(schema.has_field("id"));
                assert!(schema.has_field("name"));
                assert_eq!(schema.field_type("id"), Some(FieldType::Integer));
                assert_eq!(schema.field_type("name"), Some(FieldType::Varchar));
                assert_eq!(schema.length("name"), Some(20));
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_invalid_sql() {
        let parser = Parser::new();
        let sql = "INVALID SQL";

        assert!(parser.parse(sql).is_err());
    }

    #[test]
    fn test_parse_insert() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "INSERT INTO test_table (id, name) VALUES (1, 'Alice')";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::Insert {
                table_name,
                fields,
                values,
            } => {
                assert_eq!(table_name, "test_table");
                assert_eq!(fields, vec!["id", "name"]);
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Constant::Int(1));
                assert_eq!(values[1], Constant::String("Alice".to_string()));
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_insert_invalid() {
        let parser = Parser::new();
        let sql = "INSERT INTO test_table VALUES (1, 'Alice')"; // Missing column names

        assert!(parser.parse(sql).is_err());
    }

    #[test]
    fn test_parse_select() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "SELECT id, name FROM test_table WHERE id = 1 AND name = 'Alice'";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::Query {
                fields,
                tables,
                predicate,
            } => {
                assert_eq!(fields, vec!["id", "name"]);
                assert_eq!(tables, vec!["test_table"]);
                assert_eq!(tables, vec!["test_table"]);
                assert_eq!(
                    predicate,
                    Some(
                        Predicate::new(Term::new(
                            Expr::FieldName("id".to_owned()),
                            Expr::Constant(Constant::Int(1))
                        ))
                        .conjoin_with(Predicate::new(Term::new(
                            Expr::FieldName("name".to_owned()),
                            Expr::Constant(Constant::String("Alice".to_owned()))
                        )))
                    )
                );
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_select_simple() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "SELECT id FROM test_table";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::Query {
                fields,
                tables,
                predicate,
            } => {
                assert_eq!(fields, vec!["id"]);
                assert_eq!(tables, vec!["test_table"]);
                assert!(predicate.is_none());
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_update() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "UPDATE test_table SET name = 'Bob', age = 30 WHERE id = 1";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::Update {
                table_name,
                fields,
                values,
                predicate,
            } => {
                assert_eq!(table_name, "test_table");
                assert_eq!(fields, vec!["name", "age"]);
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Constant::String("Bob".to_string()));
                assert_eq!(values[1], Constant::Int(30));
                assert_eq!(
                    predicate,
                    Some(Predicate::new(Term::new(
                        Expr::FieldName("id".to_owned()),
                        Expr::Constant(Constant::Int(1))
                    )))
                );
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_update_no_where() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "UPDATE test_table SET age = 25";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::Update {
                table_name,
                fields,
                values,
                predicate,
            } => {
                assert_eq!(table_name, "test_table");
                assert_eq!(fields, vec!["age"]);
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], Constant::Int(25));
                assert!(predicate.is_none());
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_update_single_field() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "UPDATE users SET name = 'Alice' WHERE id = 5";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::Update {
                table_name,
                fields,
                values,
                predicate,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(fields, vec!["name"]);
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], Constant::String("Alice".to_string()));
                assert_eq!(
                    predicate,
                    Some(Predicate::new(Term::new(
                        Expr::FieldName("id".to_owned()),
                        Expr::Constant(Constant::Int(5))
                    )))
                );
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_update_invalid() {
        let parser = Parser::new();
        let sql = "UPDATE test_table SET"; // Incomplete statement

        assert!(parser.parse(sql).is_err());
    }

    #[test]
    fn test_parse_create_index() -> DbResult<()> {
        let parser = Parser::new();
        let sql = "CREATE INDEX age_idx ON test_table (age)";

        let stmt = parser.parse(sql)?;

        match stmt {
            Statement::CreateIndex {
                name,
                table_name,
                column,
            } => {
                assert_eq!(name, "age_idx");
                assert_eq!(table_name, "test_table");
                assert_eq!(column, "age");
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_create_index_invalid() {
        let parser = Parser::new();

        let sql1 = "CREATE INDEX age_idx ON test_table ()";
        assert!(parser.parse(sql1).is_err());

        let sql2 = "CREATE INDEX age_idx ON test_table (age, name)";
        assert!(parser.parse(sql2).is_err());
    }

    #[test]
    fn test_parse_create_index_variations() -> DbResult<()> {
        let parser = Parser::new();

        let sql1 = "CREATE INDEX idx_1 ON users (id)";
        let stmt1 = parser.parse(sql1)?;
        match stmt1 {
            Statement::CreateIndex {
                name,
                table_name,
                column,
            } => {
                assert_eq!(name, "idx_1");
                assert_eq!(table_name, "users");
                assert_eq!(column, "id");
            }
            _ => panic!("Unexpected statement"),
        }

        let sql2 = "CREATE INDEX name_idx ON employees (name)";
        let stmt2 = parser.parse(sql2)?;
        match stmt2 {
            Statement::CreateIndex {
                name,
                table_name,
                column,
            } => {
                assert_eq!(name, "name_idx");
                assert_eq!(table_name, "employees");
                assert_eq!(column, "name");
            }
            _ => panic!("Unexpected statement"),
        }

        Ok(())
    }
}
