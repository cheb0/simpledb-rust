use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use sqlparser::ast::{CharacterLength, DataType, Expr, SetExpr, Statement as SqlStatement, Value};

use crate::error::{DbError, DbResult};
use crate::record::schema::Schema;
use crate::query::{Constant, Expression, Term};
use crate::query::predicate::Predicate;

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable {
        table_name: String,
        schema: Schema,
    },
    Insert {
        table_name: String,
        fields: Vec<String>,
        values: Vec<Constant>,
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
            SqlStatement::Insert(insert) => self.parse_insert(insert),
            SqlStatement::Query(query) => self.parse_select(&query.body),
            _ => Err(DbError::Schema("Unsupported SQL statement".to_string())),
        }
    }

    fn parse_create_table(&self, create_table: &sqlparser::ast::CreateTable) -> DbResult<Statement> {
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

        Ok(Statement::CreateTable {
            table_name,
            schema,
        })
    }

    fn parse_insert(&self, insert: &sqlparser::ast::Insert) -> DbResult<Statement> {
        let table_name = insert.table.to_string();
        
        if insert.columns.is_empty() {
            return Err(DbError::Schema("No columns provided".to_string()));
        }
        let fields = insert.columns.iter()
            .map(|col| col.value.clone())
            .collect::<Vec<String>>();
        let source = insert.source.as_ref();

        let values = match source {
            Some(query) => {
                match &*query.body {
                    SetExpr::Values(values) => {
                        if values.rows.is_empty() {
                            return Err(DbError::Schema("No values provided for INSERT".to_string()));
                        }

                        let row = &values.rows[0];
                        row.iter()
                            .map(|expr| match expr {
                                Expr::Value(value) => {
                                    match &value.value {
                                        Value::SingleQuotedString(s) => {
                                            Ok(Constant::String(s.clone()))
                                        },
                                        Value::Number(n, _) => {
                                            Ok(Constant::Integer(n.parse().map_err(|_| {
                                                DbError::Schema(format!("Invalid integer value: {}", n))
                                            })?))
                                        },
                                        _ => Err(DbError::Schema(format!("Unsupported value type"))),
                                    }
                                }
                                _ => Err(DbError::Schema(format!(
                                    "Unsupported value type in INSERT statement"
                                ))),
                            })
                            .collect::<DbResult<Vec<Constant>>>()?
                    },
                    _ => return Err(DbError::Schema("Only VALUES clause is supported for INSERT".to_string())),
                }
            }
            _ => return Err(DbError::Schema("Only VALUES clause is supported for INSERT".to_string())),
        };

        Ok(Statement::Insert {
            table_name,
            fields,
            values,
        })
    }

    fn parse_select(&self, query: &SetExpr) -> DbResult<Statement> {
        return match query {
            SetExpr::Select(select) => {
                // Parse fields (columns)
                let fields = select.projection.iter()
                .map(|item| match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                        Ok(ident.value.clone())
                    }
                    sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                        match expr {
                            Expr::Identifier(ident) => Ok(alias.value.clone()),
                            _ => Err(DbError::Schema("Only simple column references are supported".to_string())),
                        }
                    }
                    _ => Err(DbError::Schema("Only simple column references are supported".to_string())),
                })
                .collect::<DbResult<Vec<String>>>()?;

                let tables = select.from.iter()
                    .map(|table_with_join| {
                        match &table_with_join.relation {
                            sqlparser::ast::TableFactor::Table { name, .. } => {
                                Ok(name.to_string())
                            }
                            _ => Err(DbError::Schema("Only simple table references are supported".to_string())),
                        }
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
                    predicate}
                )
            },
            _ => return Err(DbError::Schema("Only VALUES clause is supported for INSERT".to_string())),
        }
    }

    fn parse_where_clause(&self, expr: &Expr) -> DbResult<Predicate> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    sqlparser::ast::BinaryOperator::Eq => {
                        let field = match &**left {
                            Expr::Identifier(ident) => ident.value.clone(),
                            _ => return Err(DbError::Schema("Left side of = must be a field name".to_string())),
                        };
                        let value = match &**right {
                            Expr::Value(value) => match &value.value {
                                Value::SingleQuotedString(s) => Constant::String(s.clone()),
                                Value::Number(n, _) => Constant::Integer(n.parse().map_err(|_| {
                                    DbError::Schema(format!("Invalid integer value: {}", n))
                                })?),
                                _ => return Err(DbError::Schema("Unsupported value type in WHERE clause".to_string())),
                            },
                            _ => return Err(DbError::Schema("Right side of = must be a value".to_string())),
                        };
                        Ok(Predicate::default().with_term(Term::new(Expression::with_field_name(field), Expression::with_constant(value))))
                    }
                    sqlparser::ast::BinaryOperator::And => {
                        let left_pred = self.parse_where_clause(left)?;
                        let right_pred = self.parse_where_clause(right)?;
                        Ok(left_pred.conjoin_with(right_pred))
                    }
                    _ => Err(DbError::Schema("Only = and AND operators are supported in WHERE clause".to_string())),
                }
            }
            _ => Err(DbError::Schema("Unsupported expression in WHERE clause".to_string())),
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
            Statement::Insert { table_name, fields, values } => {
                assert_eq!(table_name, "test_table");
                assert_eq!(fields, vec!["id", "name"]);
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Constant::Integer(1));
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
            Statement::Query { fields, tables, predicate } => {
                assert_eq!(fields, vec!["id", "name"]);
                assert_eq!(tables, vec!["test_table"]);
                // Note: Testing predicate contents would require more complex assertions
                // as Predicate doesn't implement Eq
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
            Statement::Query { fields, tables, predicate } => {
                assert_eq!(fields, vec!["id"]);
                assert_eq!(tables, vec!["test_table"]);
            }
            _ => panic!("Unexpected statement"),
        }
        
        Ok(())
    }
}