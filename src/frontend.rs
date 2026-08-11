use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, Query, Select, SetExpr, Expr, BinaryOperator, TableFactor};
use std::sync::Arc;
use crate::execution::physical_plan::ExecutionPlan;
use crate::storage::ParquetScanExec;
use crate::execution::physical_expr::GtInt64Expr;
use crate::execution::filter::FilterExec;
use crate::execution::aggregate::HashAggregateExec;
use crate::execution::scheduler::TaskSchedulerExec;

/// This is a simplified query planner for our MVP.
/// It takes a raw SQL string and directly constructs the physical execution pipeline.
pub async fn plan_query(sql: &str) -> Result<Arc<dyn ExecutionPlan>, Box<dyn std::error::Error + Send + Sync>> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    // We expect a single SELECT query
    let statement = ast.into_iter().next().ok_or("No SQL statement found")?;
    
    match statement {
        Statement::Query(query) => plan_select(*query).await,
        _ => Err("Only SELECT queries are supported in this MVP".into()),
    }
}

async fn plan_select(query: Query) -> Result<Arc<dyn ExecutionPlan>, Box<dyn std::error::Error + Send + Sync>> {
    let select = match *query.body {
        SetExpr::Select(s) => s,
        _ => return Err("Only basic SELECT is supported".into()),
    };

    // 1. Scan Node
    // Parse the table name to scan
    let from = select.from.first().ok_or("FROM clause missing")?;
    let table_name = match &from.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err("Only simple tables supported".into()),
    };
    
    // In our simplified engine, the table name directly maps to the parquet file.
    let file_path = format!("{}.parquet", table_name);
    
    // We open the file here to read its metadata and determine how many row groups it has
    let file = tokio::fs::File::open(&file_path).await?;
    let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(file).await?;
    let metadata = builder.metadata();
    let num_row_groups = metadata.row_groups().len();

    // We will build a pipeline for EACH row group (parallelism)
    let mut pipelines: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    
    for i in 0..num_row_groups {
        let scan = Arc::new(ParquetScanExec::new(file_path.clone()).with_row_groups(vec![i]));
        pipelines.push(scan);
    }

    // 2. Filter Node
    // Parse the WHERE clause (e.g. salary > 80000)
    if let Some(selection) = select.selection {
        match selection {
            Expr::BinaryOp { left: _, op, right } => {
                if op == BinaryOperator::Gt {
                    let literal_val: i64 = match *right {
                        Expr::Value(val_with_span) => {
                            match val_with_span.value {
                                sqlparser::ast::Value::Number(num, _) => num.parse()?,
                                _ => return Err("Expected a number in WHERE clause".into()),
                            }
                        }
                        _ => return Err("Expected a literal value in WHERE clause".into()),
                    };
                    
                    let predicate = Arc::new(GtInt64Expr {
                        column_index: 2,
                        literal: literal_val,
                    });
                    
                    for pipeline in &mut pipelines {
                        *pipeline = Arc::new(FilterExec::new(predicate.clone(), pipeline.clone()));
                    }
                } else {
                    return Err("Only '>' operator supported in MVP".into());
                }
            }
            _ => return Err("Unsupported WHERE clause".into()),
        }
    }

    // 3. Aggregate Node
    // Parse GROUP BY
    let has_group_by = match select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        sqlparser::ast::GroupByExpr::All(_) => true,
        _ => false,
    };

    if has_group_by {
        for pipeline in &mut pipelines {
            *pipeline = Arc::new(HashAggregateExec::new(3, 2, pipeline.clone()));
        }
    }

    // Instead of binding 1 pipeline to 1 Tokio task, we pass all pipelines (morsels)
    // to our MorselSchedulerExec. We will use a pool of worker threads.
    // For this engine, we default to the number of physical CPU cores (e.g., 8).
    let num_workers = num_cpus::get_physical();
    
    use crate::execution::morsel_scheduler::MorselSchedulerExec;
    let final_plan: Arc<dyn ExecutionPlan> = Arc::new(MorselSchedulerExec::new(pipelines, num_workers));

    Ok(final_plan)
}
