use super::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// A physical execution node that mimics decentralized execution (like e6data's architecture).
/// It spawns an independent Tokio task (a "vCPU") to execute the child plan
/// and sends the resulting RecordBatches over a channel (mimicking in-memory network shuffle).
pub struct TaskSchedulerExec {
    pub child: Arc<dyn ExecutionPlan>,
}

impl TaskSchedulerExec {
    pub fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

#[async_trait]
impl ExecutionPlan for TaskSchedulerExec {
    async fn execute(&self) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        // We create a channel to simulate data transfer (shuffle) between compute nodes
        let (tx, rx) = mpsc::channel(100);
        
        let child = self.child.clone();
        
        // Spawn a dedicated async task (mimicking a vCPU container or remote worker)
        tokio::spawn(async move {
            match child.execute().await {
                Ok(mut child_stream) => {
                    while let Some(batch_result) = child_stream.next().await {
                        // Send the batch downstream (e.g. over the network in a real distributed system)
                        if tx.send(batch_result).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });
        
        // Return a stream that reads from the receiver
        Ok(Box::pin(ReceiverStreamWrapper {
            inner: ReceiverStream::new(rx),
        }))
    }
}

/// A wrapper around ReceiverStream to implement Stream<Item = Result<...>>
struct ReceiverStreamWrapper {
    inner: ReceiverStream<Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>>,
}

impl Stream for ReceiverStreamWrapper {
    type Item = Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
