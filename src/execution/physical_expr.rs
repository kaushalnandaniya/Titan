use arrow::record_batch::RecordBatch;
use arrow::array::{BooleanArray, Int64Array};
use std::sync::Arc;
use std::any::Any;

pub trait PhysicalExpr: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, Box<dyn std::error::Error + Send + Sync>>;
}

pub struct GtInt64Expr {
    pub column_index: usize,
    pub literal: i64,
}

impl PhysicalExpr for GtInt64Expr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, Box<dyn std::error::Error + Send + Sync>> {
        let column = batch.column(self.column_index);
        
        let scalar_array = Int64Array::from(vec![self.literal]);
        let scalar = arrow::array::Scalar::new(scalar_array);
        
        let mask = arrow_ord::cmp::gt(&column, &scalar)?;
        
        Ok(mask)
    }
}
