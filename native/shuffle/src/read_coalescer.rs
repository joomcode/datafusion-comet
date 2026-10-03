// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use datafusion::error::Result;
use std::sync::Arc;

#[derive(Debug)]
pub struct ShuffleReadCoalescer {
    target_rows: usize,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
}

impl ShuffleReadCoalescer {
    pub fn new(target_rows: usize) -> Self {
        Self {
            target_rows: target_rows.max(1),
            pending: Vec::new(),
            pending_rows: 0,
        }
    }

    pub fn buffered_rows(&self) -> usize {
        self.pending_rows
    }

    pub fn push(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let flushed = match self.pending.first() {
            Some(first) if !same_schema(first, &batch) => self.take()?,
            _ => None,
        };
        self.pending_rows += batch.num_rows();
        self.pending.push(batch);
        if flushed.is_some() {
            Ok(flushed)
        } else if self.pending_rows >= self.target_rows {
            self.take()
        } else {
            Ok(None)
        }
    }

    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        self.take()
    }

    fn take(&mut self) -> Result<Option<RecordBatch>> {
        self.pending_rows = 0;
        match self.pending.len() {
            0 => Ok(None),
            1 => Ok(self.pending.pop()),
            _ => {
                let batches = std::mem::take(&mut self.pending);
                let schema = batches[0].schema();
                Ok(Some(concat_batches(&schema, &batches)?))
            }
        }
    }
}

fn same_schema(a: &RecordBatch, b: &RecordBatch) -> bool {
    let (a, b) = (a.schema_ref(), b.schema_ref());
    Arc::ptr_eq(a, b) || a == b
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, ListArray, StringArray,
        StructArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
    use arrow::record_batch::RecordBatchOptions;

    fn schema() -> Arc<Schema> {
        let point = Fields::from(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("tag", DataType::Utf8, true),
        ]);
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("p", DataType::Struct(point), true),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
                true,
            ),
        ]))
    }

    fn block(schema: &Arc<Schema>, start: i32, rows: i32) -> RecordBatch {
        let ids: Vec<i32> = (start..start + rows).collect();
        let x: ArrayRef = Arc::new(Int64Array::from_iter(
            ids.iter().map(|i| (i % 3 != 0).then_some(*i as i64 * 10)),
        ));
        let tag: ArrayRef = Arc::new(StringArray::from_iter(
            ids.iter().map(|i| (i % 4 != 0).then(|| format!("t{i}"))),
        ));
        let DataType::Struct(point) = schema.field(1).data_type() else {
            unreachable!()
        };
        let p = StructArray::new(point.clone(), vec![x, tag], None);
        let l =
            ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(ids.iter().map(
                |i| (i % 5 != 0).then(|| (0..(*i % 3)).map(|v| Some(v as i64)).collect::<Vec<_>>()),
            ));
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(ids)), Arc::new(p), Arc::new(l)],
        )
        .unwrap()
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn drain(coalescer: &mut ShuffleReadCoalescer, blocks: Vec<RecordBatch>) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        for b in blocks {
            out.extend(coalescer.push(b).unwrap());
        }
        out.extend(coalescer.finish().unwrap());
        out
    }

    #[test]
    fn joins_small_blocks_up_to_the_target_and_keeps_row_order() {
        let schema = schema();
        let blocks: Vec<_> = (0..25).map(|i| block(&schema, i * 7, 7)).collect();
        let expected = concat_batches(&schema, &blocks).unwrap();
        let out = drain(&mut ShuffleReadCoalescer::new(50), blocks);
        assert_eq!(
            out.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![56, 56, 56, 7]
        );
        assert_eq!(concat_batches(&schema, &out).unwrap(), expected);
        assert_eq!(ids(&out[3]), (168..175).collect::<Vec<_>>());
    }

    #[test]
    fn passes_a_single_large_block_through() {
        let schema = schema();
        let large = block(&schema, 0, 100);
        let mut coalescer = ShuffleReadCoalescer::new(50);
        let out = coalescer.push(large.clone()).unwrap().unwrap();
        assert_eq!(out, large);
        assert!(coalescer.finish().unwrap().is_none());
    }

    #[test]
    fn drops_empty_blocks_and_finishes_empty() {
        let schema = schema();
        let mut coalescer = ShuffleReadCoalescer::new(10);
        assert!(coalescer.push(block(&schema, 0, 0)).unwrap().is_none());
        assert_eq!(coalescer.buffered_rows(), 0);
        assert!(coalescer.finish().unwrap().is_none());
    }

    #[test]
    fn flushes_when_the_schema_changes() {
        let plain = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let dict = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let a = RecordBatch::try_new(
            Arc::clone(&plain),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .unwrap();
        let d: DictionaryArray<Int32Type> = vec!["x", "y", "x"].into_iter().collect();
        let b = RecordBatch::try_new(Arc::clone(&dict), vec![Arc::new(d)]).unwrap();
        let c = RecordBatch::try_new(
            Arc::clone(&plain),
            vec![Arc::new(StringArray::from(vec![Some("c"), None]))],
        )
        .unwrap();
        let out = drain(
            &mut ShuffleReadCoalescer::new(100),
            vec![a.clone(), a.clone(), b.clone(), c.clone()],
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], concat_batches(&plain, &[a.clone(), a]).unwrap());
        assert_eq!(out[1], b);
        assert_eq!(out[2], c);
    }

    #[test]
    fn keeps_row_counts_of_batches_without_columns() {
        let empty = Arc::new(Schema::empty());
        let rows = |n| {
            RecordBatch::try_new_with_options(
                Arc::clone(&empty),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(n)),
            )
            .unwrap()
        };
        let out = drain(
            &mut ShuffleReadCoalescer::new(10),
            vec![rows(3), rows(4), rows(5), rows(2)],
        );
        assert_eq!(
            out.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![12, 2]
        );
        assert!(out.iter().all(|b| b.num_columns() == 0));
    }

    #[test]
    fn keeps_nulls_of_nested_columns() {
        let schema = schema();
        let blocks: Vec<_> = (0..6).map(|i| block(&schema, i * 3, 3)).collect();
        let out = drain(&mut ShuffleReadCoalescer::new(1000), blocks);
        assert_eq!(out.len(), 1);
        let p = out[0]
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(p.column(0).null_count(), 6);
        assert_eq!(p.column(1).null_count(), 5);
        assert_eq!(out[0].column(2).null_count(), 4);
    }
}
