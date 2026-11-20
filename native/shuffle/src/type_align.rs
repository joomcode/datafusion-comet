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

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, MapArray, OffsetSizeTrait,
    RecordBatch, RecordBatchOptions, StructArray,
};
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use std::sync::Arc;

pub(crate) fn align_batch_types(batch: RecordBatch, schema: &SchemaRef) -> RecordBatch {
    if batch.num_columns() != schema.fields().len() {
        return batch;
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (column, field) in batch.columns().iter().zip(schema.fields()) {
        match align_array(column, field.data_type()) {
            Some(aligned) => columns.push(aligned),
            None => return batch,
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).unwrap_or(batch)
}

fn same_type_instance(actual: &DataType, target: &DataType) -> bool {
    match (actual, target) {
        (DataType::Struct(a), DataType::Struct(t)) => {
            a.len() == t.len() && std::ptr::eq(a.as_ptr(), t.as_ptr())
        }
        (DataType::List(a), DataType::List(t))
        | (DataType::LargeList(a), DataType::LargeList(t))
        | (DataType::FixedSizeList(a, _), DataType::FixedSizeList(t, _))
        | (DataType::Map(a, _), DataType::Map(t, _)) => Arc::ptr_eq(a, t) && actual == target,
        _ if is_nested(target) => false,
        _ => actual == target,
    }
}

fn is_nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

fn align_array(array: &ArrayRef, target: &DataType) -> Option<ArrayRef> {
    if same_type_instance(array.data_type(), target) {
        return Some(Arc::clone(array));
    }
    if !array.data_type().equals_datatype(target) {
        return None;
    }
    match target {
        DataType::Struct(fields) => {
            let array = array.as_struct();
            let children = array
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(child, field)| align_array(child, field.data_type()))
                .collect::<Option<Vec<_>>>()?;
            StructArray::try_new(fields.clone(), children, array.nulls().cloned())
                .ok()
                .map(|a| Arc::new(a) as ArrayRef)
        }
        DataType::List(field) => align_list::<i32>(array.as_list::<i32>(), field),
        DataType::LargeList(field) => align_list::<i64>(array.as_list::<i64>(), field),
        DataType::FixedSizeList(field, size) => {
            let array = array.as_fixed_size_list();
            let values = align_array(array.values(), field.data_type())?;
            FixedSizeListArray::try_new(Arc::clone(field), *size, values, array.nulls().cloned())
                .ok()
                .map(|a| Arc::new(a) as ArrayRef)
        }
        DataType::Map(field, sorted) => {
            let array = array.as_map();
            let entries: ArrayRef = Arc::new(array.entries().clone());
            let entries = align_array(&entries, field.data_type())?;
            MapArray::try_new(
                Arc::clone(field),
                array.offsets().clone(),
                entries.as_struct().clone(),
                array.nulls().cloned(),
                *sorted,
            )
            .ok()
            .map(|a| Arc::new(a) as ArrayRef)
        }
        _ if array.data_type() == target => Some(Arc::clone(array)),
        _ => None,
    }
}

fn align_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Option<ArrayRef> {
    let values = align_array(array.values(), field.data_type())?;
    GenericListArray::<O>::try_new(
        Arc::clone(field),
        array.offsets().clone(),
        values,
        array.nulls().cloned(),
    )
    .ok()
    .map(|a| Arc::new(a) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Float64Array, Int32Array, Int64Array, Int64Builder, ListArray, MapBuilder, StringArray,
        StringBuilder,
    };
    use arrow::buffer::NullBuffer;
    use arrow::compute::interleave_record_batch;
    use arrow::datatypes::{Field, Fields, Schema};
    use std::collections::HashMap;

    fn money_type(metadata: Option<(&str, &str)>) -> DataType {
        let amount = Field::new("amount", DataType::Float64, true);
        let amount = match metadata {
            Some((k, v)) => amount.with_metadata(HashMap::from([(k.to_string(), v.to_string())])),
            None => amount,
        };
        DataType::Struct(Fields::from(vec![
            amount,
            Field::new("ccy", DataType::Utf8, true),
        ]))
    }

    fn list_type() -> DataType {
        DataType::List(Arc::new(Field::new("element", DataType::Int64, true)))
    }

    fn map_type() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        )
    }

    fn schema(metadata: Option<(&str, &str)>) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "costs",
                DataType::Struct(Fields::from(vec![
                    Field::new("a", money_type(metadata), true),
                    Field::new("b", money_type(metadata), true),
                ])),
                true,
            ),
            Field::new("l", list_type(), true),
            Field::new("m", map_type(), true),
        ]))
    }

    fn money(data_type: &DataType, base: i32, nulls: &[bool]) -> ArrayRef {
        let DataType::Struct(fields) = data_type else {
            unreachable!()
        };
        let n = nulls.len();
        Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Float64Array::from_iter_values(
                    (0..n).map(|i| (base + i as i32) as f64),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..n).map(|i| format!("c{}", base + i as i32)),
                )),
            ],
            Some(NullBuffer::from(nulls.to_vec())),
        ))
    }

    fn batch(schema: &SchemaRef, base: i32) -> RecordBatch {
        let DataType::Struct(costs) = schema.field(1).data_type() else {
            unreachable!()
        };
        let nulls = [true, false, true];
        let costs = StructArray::new(
            costs.clone(),
            vec![
                money(costs[0].data_type(), base, &nulls),
                money(costs[1].data_type(), base + 10, &[false, true, true]),
            ],
            Some(NullBuffer::from(vec![true, true, false])),
        );
        let DataType::List(element) = schema.field(2).data_type() else {
            unreachable!()
        };
        let list = ListArray::new(
            Arc::clone(element),
            arrow::buffer::OffsetBuffer::from_lengths([2, 0, 1]),
            Arc::new(Int64Array::from(vec![Some(base as i64), None, Some(7)])),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let DataType::Map(entries, _) = schema.field(3).data_type() else {
            unreachable!()
        };
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new())
            .with_keys_field(Field::new("keys", DataType::Utf8, false))
            .with_values_field(Field::new("values", DataType::Int64, true));
        builder.keys().append_value(format!("k{base}"));
        builder.values().append_value(base as i64);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.keys().append_value("x");
        builder.values().append_null();
        builder.append(true).unwrap();
        let built = builder.finish();
        let map = MapArray::new(
            Arc::clone(entries),
            built.offsets().clone(),
            built.entries().clone(),
            built.nulls().cloned(),
            false,
        );
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(vec![base, base + 1, base + 2])),
                Arc::new(costs),
                Arc::new(list),
                Arc::new(map),
            ],
        )
        .unwrap()
    }

    fn assert_shares_types(batch: &RecordBatch, schema: &SchemaRef) {
        assert!(Arc::ptr_eq(&batch.schema(), schema));
        for (column, field) in batch.columns().iter().zip(schema.fields()) {
            assert!(same_type_instance(column.data_type(), field.data_type()));
        }
        let costs = batch.column(1).as_struct();
        let DataType::Struct(target) = schema.field(1).data_type() else {
            unreachable!()
        };
        for (child, field) in costs.columns().iter().zip(target.iter()) {
            assert!(same_type_instance(child.data_type(), field.data_type()));
        }
    }

    #[test]
    fn equal_types_from_other_instances_take_the_writer_types_without_copying() {
        let writer = schema(None);
        let other = schema(None);
        assert!(!Arc::ptr_eq(&writer, &other));
        let input = batch(&other, 0);
        let aligned = align_batch_types(input.clone(), &writer);
        assert_shares_types(&aligned, &writer);
        assert_eq!(aligned.columns(), input.columns());
        let before = input
            .column(1)
            .as_struct()
            .column(0)
            .as_struct()
            .column(1)
            .to_data();
        let after = aligned
            .column(1)
            .as_struct()
            .column(0)
            .as_struct()
            .column(1)
            .to_data();
        assert_eq!(before.buffers()[1].as_ptr(), after.buffers()[1].as_ptr());
    }

    #[test]
    fn field_metadata_differences_align_to_the_writer_metadata() {
        let writer = schema(Some(("PARQUET:field_id", "1")));
        let other = schema(Some(("PARQUET:field_id", "2")));
        let input = batch(&other, 5);
        let aligned = align_batch_types(input.clone(), &writer);
        assert_shares_types(&aligned, &writer);
        for (a, b) in aligned.columns().iter().zip(input.columns()) {
            assert_eq!(a.to_data().buffers(), b.to_data().buffers());
            assert_eq!(a.len(), b.len());
            assert_eq!(a.null_count(), b.null_count());
        }
    }

    #[test]
    fn sliced_input_aligns() {
        let writer = schema(None);
        let input = batch(&schema(None), 3).slice(1, 2);
        let aligned = align_batch_types(input.clone(), &writer);
        assert_shares_types(&aligned, &writer);
        assert_eq!(aligned.columns(), input.columns());
    }

    #[test]
    fn mixed_instances_interleave_after_alignment() {
        let writer = schema(None);
        let batches: Vec<RecordBatch> = (0..4)
            .map(|i| align_batch_types(batch(&schema(None), i * 100), &writer))
            .collect();
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        let out = interleave_record_batch(&refs, &[(3, 2), (0, 0), (2, 1), (0, 2)]).unwrap();
        let expected = interleave_record_batch(
            &[
                &batch(&writer, 300),
                &batch(&writer, 0),
                &batch(&writer, 200),
            ],
            &[(0, 2), (1, 0), (2, 1), (1, 2)],
        )
        .unwrap();
        assert_eq!(out, expected);
    }

    #[test]
    fn incompatible_types_keep_the_input() {
        let writer = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let aligned = align_batch_types(input.clone(), &writer);
        assert!(Arc::ptr_eq(&aligned.schema(), &input.schema()));
    }

    #[test]
    fn nulls_under_a_non_nullable_writer_field_keep_the_input() {
        let nullable = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
            true,
        )]));
        let strict = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, false)])),
            true,
        )]));
        let DataType::Struct(fields) = nullable.field(0).data_type() else {
            unreachable!()
        };
        let input = RecordBatch::try_new(
            Arc::clone(&nullable),
            vec![Arc::new(StructArray::new(
                fields.clone(),
                vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
                None,
            ))],
        )
        .unwrap();
        let aligned = align_batch_types(input.clone(), &strict);
        assert!(Arc::ptr_eq(&aligned.schema(), &nullable));
    }
}
