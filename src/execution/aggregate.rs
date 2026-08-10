use super::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use hashbrown::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A physical execution plan node that groups data by a string column
/// and computes the SUM of an integer column.
pub struct HashAggregateExec {
    pub group_col_index: usize,
    pub sum_col_index: usize,
    pub child: Arc<dyn ExecutionPlan>,
}

impl HashAggregateExec {
    pub fn new(group_col_index: usize, sum_col_index: usize, child: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            group_col_index,
            sum_col_index,
            child,
        }
    }
}

#[async_trait]
impl ExecutionPlan for HashAggregateExec {
    async fn execute(&self) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let mut child_stream = self.child.execute().await?;
        
        // This is a blocking operator. We must consume the entire child stream
        // to compute the final aggregation before we can yield any output.
        let mut hash_table: HashMap<String, i64> = HashMap::new();
        
        while let Some(batch_result) = child_stream.next().await {
            let batch = batch_result?;
            
            // 1. Get the grouping column (Utf8)
            let group_col = batch.column(self.group_col_index);
            let string_array = group_col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "Group column is not a StringArray")) as Box<dyn std::error::Error + Send + Sync>)?;
            
            // 2. Get the aggregation column (Int64)
            let sum_col = batch.column(self.sum_col_index);
            let int_array = sum_col.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "Sum column is not an Int64Array")) as Box<dyn std::error::Error + Send + Sync>)?;
            
            // 3. Iterate and accumulate in the hash table
            for i in 0..batch.num_rows() {
                if string_array.is_null(i) || int_array.is_null(i) {
                    continue;
                }
                
                let group_key = string_array.value(i).to_string();
                let sum_val = int_array.value(i);
                
                *hash_table.entry(group_key).or_insert(0) += sum_val;
            }
        }
        
        // 4. We've consumed the entire stream. Now we build the output RecordBatch.
        let mut output_groups = Vec::new();
        let mut output_sums = Vec::new();
        
        for (group, sum) in hash_table {
            output_groups.push(group);
            output_sums.push(sum);
        }
        
        let out_group_array = StringArray::from(output_groups);
        let out_sum_array = Int64Array::from(output_sums);
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("department", DataType::Utf8, false),
            Field::new("total_salary", DataType::Int64, false),
        ]));
        
        let final_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(out_group_array) as ArrayRef, Arc::new(out_sum_array) as ArrayRef],
        )?;
        
        Ok(Box::pin(AggregateStream {
            batch: Some(final_batch),
        }))
    }
}

/// A simple stream that yields a single RecordBatch and then terminates.
struct AggregateStream {
    batch: Option<RecordBatch>,
}

impl Stream for AggregateStream {
    type Item = Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(b) = self.batch.take() {
            Poll::Ready(Some(Ok(b)))
        } else {
            Poll::Ready(None)
        }
    }
}

/// A physical execution plan node that merges partial aggregations
/// from multiple parallel partitions (Map-Reduce style).
pub struct MergeAggregateExec {
    pub children: Vec<Arc<dyn ExecutionPlan>>,
}

impl MergeAggregateExec {
    pub fn new(children: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        Self { children }
    }
}

#[async_trait]
impl ExecutionPlan for MergeAggregateExec {
    async fn execute(&self) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let mut global_hash_table: HashMap<String, i64> = HashMap::new();

        // Start executing all children
        let mut child_streams = Vec::new();
        for child in &self.children {
            child_streams.push(child.execute().await?);
        }

        // We process all batches from all partitions
        for mut stream in child_streams {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                
                // Expecting department (0), total_salary (1)
                let group_col = batch.column(0);
                let string_array = group_col.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "Group column is not a StringArray")) as Box<dyn std::error::Error + Send + Sync>)?;
                
                let sum_col = batch.column(1);
                let int_array = sum_col.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "Sum column is not an Int64Array")) as Box<dyn std::error::Error + Send + Sync>)?;
                
                for i in 0..batch.num_rows() {
                    if string_array.is_null(i) || int_array.is_null(i) {
                        continue;
                    }
                    
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
        
        let final_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(out_group_array) as ArrayRef, Arc::new(out_sum_array) as ArrayRef],
        )?;
        
        Ok(Box::pin(AggregateStream {
            batch: Some(final_batch),
        }))
    }
}
