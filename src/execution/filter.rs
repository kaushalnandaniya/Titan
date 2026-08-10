use super::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use super::physical_expr::PhysicalExpr;
use arrow::compute::filter_record_batch;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A physical execution plan node that filters rows from its child plan
/// using SIMD-accelerated compute kernels.
pub struct FilterExec {
    pub predicate: Arc<dyn PhysicalExpr>,
    pub child: Arc<dyn ExecutionPlan>,
}

impl FilterExec {
    pub fn new(predicate: Arc<dyn PhysicalExpr>, child: Arc<dyn ExecutionPlan>) -> Self {
        Self { predicate, child }
    }
}

use async_trait::async_trait;

#[async_trait]
impl ExecutionPlan for FilterExec {
    async fn execute(&self) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let child_stream = self.child.execute().await?;
        let predicate = self.predicate.clone();
        
        Ok(Box::pin(FilterStream {
            child_stream,
            predicate,
        }))
    }
}

/// A Stream that polls its child stream, applies the filter predicate,
/// and yields only the matching RecordBatches.
struct FilterStream {
    child_stream: SendableRecordBatchStream,
    predicate: Arc<dyn PhysicalExpr>,
}

impl Stream for FilterStream {
    // The Parquet scanner returns parquet::errors::ParquetError, so we map to Box<dyn Error> 
    // to be generic. 
    type Item = Result<arrow::record_batch::RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Continuously poll the child stream until we get a batch with rows (after filtering)
        // or the stream ends.
        loop {
            match futures::ready!(self.child_stream.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    // Evaluate the predicate to get the boolean mask
                    let mask = match self.predicate.evaluate(&batch) {
                        Ok(mask) => mask,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };

                    // Apply the filter using Arrow's highly optimized filter kernel
                    let filtered_batch = match filter_record_batch(&batch, &mask) {
                        Ok(b) => b,
                        Err(e) => return Poll::Ready(Some(Err(Box::new(e)))),
                    };

                    // If the batch is empty after filtering, try polling for the next one
                    if filtered_batch.num_rows() > 0 {
                        return Poll::Ready(Some(Ok(filtered_batch)));
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }
    }
}
