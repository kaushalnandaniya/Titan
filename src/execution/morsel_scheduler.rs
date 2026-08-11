use super::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use arrow::record_batch::RecordBatch;
use arrow::array::{StringArray, Int64Array, ArrayRef, Array};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_stream::wrappers::ReceiverStream;
use hashbrown::HashMap;

/// A Morsel-Driven Work Stealing Scheduler.
/// It takes a list of independent partitions (pipelines) and uses a fixed pool of worker
/// threads to execute them. Fast workers will steal more work from the queue,
/// completely avoiding the straggler problem caused by asymmetrical CPU cores.
pub struct MorselSchedulerExec {
    pub partitions: Vec<Arc<dyn ExecutionPlan>>,
    pub num_workers: usize,
}

impl MorselSchedulerExec {
    pub fn new(partitions: Vec<Arc<dyn ExecutionPlan>>, num_workers: usize) -> Self {
        Self { partitions, num_workers }
    }
}

#[async_trait]
impl ExecutionPlan for MorselSchedulerExec {
    async fn execute(&self) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let (work_tx, work_rx) = async_channel::unbounded::<Arc<dyn ExecutionPlan>>();
        let (result_tx, result_rx) = async_channel::unbounded::<Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>>();

        // Push all "morsels" (partitions) into the work queue
        for partition in &self.partitions {
            work_tx.send(partition.clone()).await.unwrap();
        }
        work_tx.close(); // No more work will be added

        // Spawn a fixed pool of N worker threads (representing physical cores)
        for _ in 0..self.num_workers {
            let rx = work_rx.clone();
            let tx = result_tx.clone();
            
            tokio::spawn(async move {
                // Workers continuously pull work from the queue until it's empty.
                // This is dynamic work stealing! P-Cores will naturally process more partitions.
                while let Ok(pipeline) = rx.recv().await {
                    match pipeline.execute().await {
                        Ok(mut stream) => {
                            while let Some(batch_result) = stream.next().await {
                                if tx.send(batch_result).await.is_err() {
                                    return; // Receiver closed
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                        }
                    }
                }
            });
        }
        
        // Drop the original result_tx so the channel closes when all workers finish and drop their clones
        drop(result_tx);

        // We run the global Merge phase directly in the returning stream.
        // We will read all partial aggregates from the result_rx and merge them.
        // To keep it clean, we spawn a single aggregator task that reads from result_rx
        // and pushes the final batch to a standard mpsc channel that we return as a stream.
        
        let (final_tx, final_rx) = tokio::sync::mpsc::channel(1);
        
        tokio::spawn(async move {
            let mut global_hash_table: HashMap<String, i64> = HashMap::new();
            
            while let Ok(batch_result) = result_rx.recv().await {
                if let Ok(batch) = batch_result {
                    let group_col = batch.column(0);
                    let string_array = group_col.as_any().downcast_ref::<StringArray>().unwrap();
                    let sum_col = batch.column(1);
                    let int_array = sum_col.as_any().downcast_ref::<Int64Array>().unwrap();
                    
                    for i in 0..batch.num_rows() {
                        if string_array.is_null(i) || int_array.is_null(i) { continue; }
                        let group_key = string_array.value(i).to_string();
                        let sum_val = int_array.value(i);
                        *global_hash_table.entry(group_key).or_insert(0) += sum_val;
                    }
                }
            }
            
            // Build the final output batch
            let mut output_groups = Vec::new();
            let mut output_sums = Vec::new();
            for (group, sum) in global_hash_table {
                output_groups.push(group);
                output_sums.push(sum);
            }
            
            let out_group_array = StringArray::from(output_groups);
            let out_sum_array = Int64Array::from(output_sums);
            
            let schema = Arc::new(Schema::new(vec![
                Field::new("department", DataType::Utf8, false),
                Field::new("total_salary", DataType::Int64, false),
            ]));
            
            if let Ok(final_batch) = RecordBatch::try_new(
                schema,
                vec![Arc::new(out_group_array) as ArrayRef, Arc::new(out_sum_array) as ArrayRef],
            ) {
                let _ = final_tx.send(Ok(final_batch)).await;
            }
        });

        Ok(Box::pin(MorselStream {
            inner: ReceiverStream::new(final_rx),
        }))
    }
}

struct MorselStream {
    inner: ReceiverStream<Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>>,
}

impl Stream for MorselStream {
    type Item = Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
