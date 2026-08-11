use mimalloc::MiMalloc;
use std::env;
use std::error::Error;
use std::time::Instant;
use tokio::runtime::Builder;
use futures::StreamExt;
use titan::frontend::plan_query;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: benchmark <table_name> <num_threads>");
        std::process::exit(1);
    }
    
    let table_name = &args[1];
    let num_threads: usize = args[2].parse().unwrap_or(4);
    
    // Build a custom Tokio runtime to test core scaling (Amdahl's law)
    let rt = Builder::new_multi_thread()
        .worker_threads(num_threads)
        .enable_all()
        .build()?;
        
    rt.block_on(async {
        let sql = format!("SELECT department, SUM(salary) FROM {} WHERE salary > 80000 GROUP BY department", table_name);
        
        let start_plan = Instant::now();
        let plan = plan_query(&sql).await?;
        let plan_time = start_plan.elapsed();
        
        let start_exec = Instant::now();
        let mut stream = plan.execute().await?;
        
        let mut total_rows = 0;
        let mut total_batches = 0;
        
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows();
            total_batches += 1;
        }
        
        let exec_time = start_exec.elapsed();
        let total_time = start_plan.elapsed();
        
        println!("Benchmark Results for {}:", table_name);
        println!("Threads used: {}", num_threads);
        println!("Planning Time: {:?}", plan_time);
        println!("Execution Time: {:?}", exec_time);
        println!("Total Time: {:?}", total_time);
        println!("Output Rows: {}", total_rows);
        println!("Output Batches: {}", total_batches);
        println!("--------------------------------------------------");
        
        Ok(())
    })
}
