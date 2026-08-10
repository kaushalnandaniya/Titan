use futures::StreamExt;
use std::error::Error;
use titan::frontend::plan_query;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Titan Lakehouse Engine - Starting...");
    
    // The raw SQL query to run
    let sql = "SELECT department, SUM(salary) FROM test_data WHERE salary > 80000 GROUP BY department";
    println!("Executing SQL: {}\n", sql);
    
    // 1. Plan the query (AST -> Physical Plan)
    let plan = plan_query(sql).await?;
    
    // 2. Execute the plan
    let mut stream = plan.execute().await?;
    
    let mut total_rows = 0;
    let mut num_batches = 0;
    
    println!("Scanning Parquet file...");
    
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        num_batches += 1;
        
        // Let's print the aggregated output
        println!("Aggregation Output:");
        println!("Schema: {:?}", batch.schema());
        
        let dept_col = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let sum_col = batch.column(1).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        
        for i in 0..batch.num_rows() {
            println!("  {}: {}", dept_col.value(i), sum_col.value(i));
        }
    }
    
    println!("Scan Complete!");
    println!("Total Batches: {}", num_batches);
    println!("Total Rows: {}", total_rows);
    
    Ok(())
}
