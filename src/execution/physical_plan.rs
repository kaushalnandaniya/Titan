// Physical plan traits
use arrow::record_batch::RecordBatch;
use futures::Stream;
use std::pin::Pin;

use async_trait::async_trait;

pub type SendableRecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>> + Send>>;

#[async_trait]
pub trait ExecutionPlan: Send + Sync {
    async fn execute(&self) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>>;
}
