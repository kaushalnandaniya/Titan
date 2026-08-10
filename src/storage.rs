use arrow::record_batch::RecordBatch;
use futures::Stream;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::pin::Pin;
use tokio::fs::File;


/// ParquetScanExec is responsible for reading Parquet files asynchronously
/// and yielding Arrow RecordBatches. This is the foundation of our vectorized
/// execution engine.
pub struct ParquetScanExec {
    pub file_path: String,
    pub row_groups: Option<Vec<usize>>,
}

impl ParquetScanExec {
    pub fn new(file_path: impl Into<String>) -> Self {
        Self {
            file_path: file_path.into(),
            row_groups: None,
        }
    }
    
    pub fn with_row_groups(mut self, row_groups: Vec<usize>) -> Self {
        self.row_groups = Some(row_groups);
        self
    }
}

use crate::execution::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use async_trait::async_trait;

#[async_trait]
impl ExecutionPlan for ParquetScanExec {
    async fn execute(&self) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let file = File::open(&self.file_path).await?;
        
        let mut builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        
        if let Some(ref rg) = self.row_groups {
            builder = builder.with_row_groups(rg.clone());
        }
        
        let stream = builder.with_batch_size(8192).build()?;
        
        // We map the ParquetError to Box<dyn Error + Send + Sync> to match our trait
        use futures::StreamExt;
        let mapped_stream = stream.map(|res| res.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>));
        
        Ok(Box::pin(mapped_stream))
    }
}
