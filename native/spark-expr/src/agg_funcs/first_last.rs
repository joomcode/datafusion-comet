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

use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, GenericByteArray, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::nullif;
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::{
    BinaryType, ByteArrayType, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    Field, FieldRef, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    LargeBinaryType, LargeUtf8Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    Utf8Type,
};
use datafusion::common::{exec_err, internal_err, Result, ScalarValue};
use datafusion::functions_aggregate::first_last::{FirstValue, LastValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFirstLast {
    name: String,
    signature: Signature,
    is_first: bool,
}

impl SparkFirstLast {
    pub fn first() -> Self {
        Self::new("first", true)
    }

    pub fn last() -> Self {
        Self::new("last", false)
    }

    fn new(name: &str, is_first: bool) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::any(1, Volatility::Immutable),
            is_first,
        }
    }

    fn inner(&self) -> Box<dyn AggregateUDFImpl> {
        if self.is_first {
            Box::new(FirstValue::new())
        } else {
            Box::new(LastValue::new())
        }
    }

    pub fn groups_supported_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
        )
    }
}

impl AggregateUDFImpl for SparkFirstLast {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(
            Field::new(self.name(), arg_fields[0].data_type().clone(), true)
                .with_metadata(arg_fields[0].metadata().clone()),
        ))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner().accumulator(acc_args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner().state_fields(args)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        args.order_bys.is_empty()
            && !args.is_distinct
            && Self::groups_supported_type(args.return_field.data_type())
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.return_field.data_type().clone();
        let is_first = self.is_first;
        let ignore_nulls = args.ignore_nulls;

        macro_rules! primitive {
            ($t:ty) => {
                Ok(Box::new(FirstLastGroupsAccumulator::new(
                    PrimitiveStore::<$t>::new(data_type),
                    is_first,
                    ignore_nulls,
                )))
            };
        }
        macro_rules! bytes {
            ($t:ty) => {
                Ok(Box::new(FirstLastGroupsAccumulator::new(
                    BytesStore::<$t>::new(),
                    is_first,
                    ignore_nulls,
                )))
            };
        }

        match &data_type {
            DataType::Boolean => Ok(Box::new(FirstLastGroupsAccumulator::new(
                BooleanStore::default(),
                is_first,
                ignore_nulls,
            ))),
            DataType::Int8 => primitive!(Int8Type),
            DataType::Int16 => primitive!(Int16Type),
            DataType::Int32 => primitive!(Int32Type),
            DataType::Int64 => primitive!(Int64Type),
            DataType::UInt8 => primitive!(UInt8Type),
            DataType::UInt16 => primitive!(UInt16Type),
            DataType::UInt32 => primitive!(UInt32Type),
            DataType::UInt64 => primitive!(UInt64Type),
            DataType::Float32 => primitive!(Float32Type),
            DataType::Float64 => primitive!(Float64Type),
            DataType::Decimal128(_, _) => primitive!(Decimal128Type),
            DataType::Decimal256(_, _) => primitive!(Decimal256Type),
            DataType::Date32 => primitive!(Date32Type),
            DataType::Date64 => primitive!(Date64Type),
            DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType),
            DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType),
            DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType),
            DataType::Utf8 => bytes!(Utf8Type),
            DataType::LargeUtf8 => bytes!(LargeUtf8Type),
            DataType::Binary => bytes!(BinaryType),
            DataType::LargeBinary => bytes!(LargeBinaryType),
            other => internal_err!("{} groups accumulator does not support {other}", self.name),
        }
    }
}

trait ValueStore: Send + 'static {
    type Input: Array + 'static;

    fn downcast(array: &ArrayRef) -> Result<&Self::Input> {
        match array.as_any().downcast_ref::<Self::Input>() {
            Some(input) => Ok(input),
            None => internal_err!("unexpected input type {}", array.data_type()),
        }
    }

    fn resize(&mut self, total_num_groups: usize);

    fn set(&mut self, group: usize, input: &Self::Input, row: usize);

    fn emit(&mut self, emit_to: EmitTo) -> Result<ArrayRef>;

    fn size(&self) -> usize;
}

struct PrimitiveStore<T: ArrowPrimitiveType> {
    data_type: DataType,
    values: Vec<T::Native>,
    valid: Vec<bool>,
}

impl<T: ArrowPrimitiveType> PrimitiveStore<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            values: Vec::new(),
            valid: Vec::new(),
        }
    }
}

impl<T: ArrowPrimitiveType + Send> ValueStore for PrimitiveStore<T> {
    type Input = PrimitiveArray<T>;

    fn resize(&mut self, total_num_groups: usize) {
        if total_num_groups > self.values.len() {
            self.values.resize(total_num_groups, T::Native::default());
            self.valid.resize(total_num_groups, false);
        }
    }

    #[inline]
    fn set(&mut self, group: usize, input: &Self::Input, row: usize) {
        self.values[group] = input.values()[row];
        self.valid[group] = input.is_valid(row);
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.values);
        let valid = emit_to.take_needed(&mut self.valid);
        Ok(Arc::new(
            PrimitiveArray::<T>::new(ScalarBuffer::from(values), Some(NullBuffer::from(valid)))
                .with_data_type(self.data_type.clone()),
        ))
    }

    fn size(&self) -> usize {
        self.values.capacity() * size_of::<T::Native>() + self.valid.capacity()
    }
}

#[derive(Default)]
struct BooleanStore {
    values: Vec<bool>,
    valid: Vec<bool>,
}

impl ValueStore for BooleanStore {
    type Input = BooleanArray;

    fn resize(&mut self, total_num_groups: usize) {
        if total_num_groups > self.values.len() {
            self.values.resize(total_num_groups, false);
            self.valid.resize(total_num_groups, false);
        }
    }

    #[inline]
    fn set(&mut self, group: usize, input: &Self::Input, row: usize) {
        self.values[group] = input.values().value(row);
        self.valid[group] = input.is_valid(row);
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.values);
        let valid = emit_to.take_needed(&mut self.valid);
        Ok(Arc::new(BooleanArray::new(
            BooleanBuffer::from(values),
            Some(NullBuffer::from(valid)),
        )))
    }

    fn size(&self) -> usize {
        self.values.capacity() + self.valid.capacity()
    }
}

struct BytesStore<T: ByteArrayType> {
    values: Vec<Vec<u8>>,
    valid: Vec<bool>,
    heap: usize,
    phantom: PhantomData<T>,
}

impl<T: ByteArrayType> BytesStore<T> {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            valid: Vec::new(),
            heap: 0,
            phantom: PhantomData,
        }
    }
}

impl<T: ByteArrayType> ValueStore for BytesStore<T> {
    type Input = GenericByteArray<T>;

    fn resize(&mut self, total_num_groups: usize) {
        if total_num_groups > self.values.len() {
            self.values.resize_with(total_num_groups, Vec::new);
            self.valid.resize(total_num_groups, false);
        }
    }

    #[inline]
    fn set(&mut self, group: usize, input: &Self::Input, row: usize) {
        if input.is_null(row) {
            self.valid[group] = false;
            return;
        }
        let bytes: &[u8] = input.value(row).as_ref();
        let slot = &mut self.values[group];
        let before = slot.capacity();
        slot.clear();
        slot.extend_from_slice(bytes);
        self.heap += slot.capacity() - before;
        self.valid[group] = true;
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.values);
        let valid = emit_to.take_needed(&mut self.valid);

        let mut total = 0usize;
        let mut released = 0usize;
        for (value, &ok) in values.iter().zip(valid.iter()) {
            if ok {
                total += value.len();
            }
            released += value.capacity();
        }
        self.heap -= released;
        if T::Offset::from_usize(total).is_none() {
            return exec_err!(
                "{} values of {total} bytes overflow the offsets of {}",
                values.len(),
                T::DATA_TYPE
            );
        }

        let mut offsets: Vec<T::Offset> = Vec::with_capacity(values.len() + 1);
        let mut data: Vec<u8> = Vec::with_capacity(total);
        offsets.push(T::Offset::usize_as(0));
        for (value, &ok) in values.iter().zip(valid.iter()) {
            if ok {
                data.extend_from_slice(value);
            }
            offsets.push(T::Offset::usize_as(data.len()));
        }

        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let array = unsafe {
            GenericByteArray::<T>::new_unchecked(
                offsets,
                Buffer::from_vec(data),
                Some(NullBuffer::from(valid)),
            )
        };
        Ok(Arc::new(array))
    }

    fn size(&self) -> usize {
        self.values.capacity() * size_of::<Vec<u8>>() + self.heap + self.valid.capacity()
    }
}

struct FirstLastGroupsAccumulator<S: ValueStore> {
    store: S,
    is_set: Vec<bool>,
    is_first: bool,
    ignore_nulls: bool,
}

impl<S: ValueStore> FirstLastGroupsAccumulator<S> {
    fn new(store: S, is_first: bool, ignore_nulls: bool) -> Self {
        Self {
            store,
            is_set: Vec::new(),
            is_first,
            ignore_nulls,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        if total_num_groups > self.is_set.len() {
            self.is_set.resize(total_num_groups, false);
        }
        self.store.resize(total_num_groups);
    }

    fn apply(
        &mut self,
        input: &S::Input,
        group_indices: &[usize],
        mask: Option<&BooleanBuffer>,
        skip_nulls: bool,
    ) {
        let skip_nulls = skip_nulls && input.null_count() > 0;
        for (row, &group) in group_indices.iter().enumerate() {
            if self.is_first && self.is_set[group] {
                continue;
            }
            if let Some(mask) = mask {
                if !mask.value(row) {
                    continue;
                }
            }
            if skip_nulls && input.is_null(row) {
                continue;
            }
            self.store.set(group, input, row);
            self.is_set[group] = true;
        }
    }
}

fn true_mask(array: &BooleanArray) -> BooleanBuffer {
    match array.nulls() {
        Some(nulls) => array.values() & nulls.inner(),
        None => array.values().clone(),
    }
}

impl<S: ValueStore> GroupsAccumulator for FirstLastGroupsAccumulator<S> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let input = S::downcast(&values[0])?;
        let mask = opt_filter.map(true_mask);
        self.apply(input, group_indices, mask.as_ref(), self.ignore_nulls);
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let input = S::downcast(&values[0])?;
        let mask = true_mask(values[1].as_boolean());
        self.apply(input, group_indices, Some(&mask), false);
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = self.store.emit(emit_to)?;
        emit_to.take_needed(&mut self.is_set);
        Ok(values)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let values = self.store.emit(emit_to)?;
        let is_set = emit_to.take_needed(&mut self.is_set);
        Ok(vec![
            values,
            Arc::new(BooleanArray::new(BooleanBuffer::from(is_set), None)),
        ])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let input = &values[0];
        let mut is_set = match opt_filter {
            Some(filter) => true_mask(filter),
            None => BooleanBuffer::new_set(input.len()),
        };
        if self.ignore_nulls {
            if let Some(nulls) = input.logical_nulls() {
                is_set = &is_set & nulls.inner();
            }
        }
        let value = if is_set.count_set_bits() == input.len() {
            Arc::clone(input)
        } else {
            nullif(input.as_ref(), &BooleanArray::new(!&is_set, None))?
        };
        Ok(vec![value, Arc::new(BooleanArray::new(is_set, None))])
    }

    fn size(&self) -> usize {
        self.store.size() + self.is_set.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::Schema;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::PhysicalExpr;

    fn accumulator(
        udaf: &SparkFirstLast,
        data_type: DataType,
        ignore_nulls: bool,
    ) -> Box<dyn GroupsAccumulator> {
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), true)]);
        let expr: Arc<dyn PhysicalExpr> = col("a", &schema).unwrap();
        let return_field: FieldRef = Arc::new(Field::new("f", data_type.clone(), true));
        let expr_field: FieldRef = Arc::new(Field::new("a", data_type, true));
        let args = AccumulatorArgs {
            return_field,
            schema: &schema,
            expr_fields: &[expr_field],
            ignore_nulls,
            order_bys: &[],
            is_reversed: false,
            name: "f",
            is_distinct: false,
            exprs: &[expr],
        };
        assert!(udaf.groups_accumulator_supported(args.clone()));
        udaf.create_groups_accumulator(args).unwrap()
    }

    fn ints(values: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(values))
    }

    fn strings(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    fn filter(values: Vec<Option<bool>>) -> BooleanArray {
        BooleanArray::from(values)
    }

    fn as_ints(array: &ArrayRef) -> Vec<Option<i32>> {
        array.as_primitive::<Int32Type>().iter().collect()
    }

    fn as_strings(array: &ArrayRef) -> Vec<Option<String>> {
        array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(|s| s.to_string()))
            .collect()
    }

    fn as_bools(array: &ArrayRef) -> Vec<Option<bool>> {
        array.as_boolean().iter().collect()
    }

    fn reference(
        is_first: bool,
        ignore_nulls: bool,
        values: &[Option<i32>],
        groups: &[usize],
        mask: &[Option<bool>],
        num_groups: usize,
    ) -> Vec<Option<i32>> {
        let mut out: Vec<(Option<i32>, bool)> = vec![(None, false); num_groups];
        for ((v, &g), m) in values.iter().zip(groups).zip(mask) {
            if *m != Some(true) || (ignore_nulls && v.is_none()) {
                continue;
            }
            if is_first && out[g].1 {
                continue;
            }
            out[g] = (*v, true);
        }
        out.into_iter().map(|(v, _)| v).collect()
    }

    #[test]
    fn first_and_last_respect_nulls() {
        let values = ints(vec![None, Some(1), Some(2), None, None, Some(5)]);
        let groups = [0, 0, 1, 1, 2, 2];
        let cases = [
            (true, false, vec![None, Some(2), None]),
            (true, true, vec![Some(1), Some(2), Some(5)]),
            (false, false, vec![Some(1), None, Some(5)]),
            (false, true, vec![Some(1), Some(2), Some(5)]),
        ];
        for (is_first, ignore_nulls, expected) in cases {
            let udaf = if is_first {
                SparkFirstLast::first()
            } else {
                SparkFirstLast::last()
            };
            let mut acc = accumulator(&udaf, DataType::Int32, ignore_nulls);
            acc.update_batch(&[Arc::clone(&values)], &groups, None, 4)
                .unwrap();
            let out = acc.evaluate(EmitTo::All).unwrap();
            let mut expected = expected;
            expected.push(None);
            assert_eq!(
                as_ints(&out),
                expected,
                "first={is_first} ignore={ignore_nulls}"
            );
        }
    }

    #[test]
    fn filter_skips_rows_including_null_filter_values() {
        let values = ints(vec![Some(1), Some(2), Some(3), Some(4)]);
        let groups = [0, 0, 0, 0];
        let mask = filter(vec![Some(false), None, Some(true), Some(false)]);
        let mut first = accumulator(&SparkFirstLast::first(), DataType::Int32, true);
        first
            .update_batch(&[Arc::clone(&values)], &groups, Some(&mask), 1)
            .unwrap();
        assert_eq!(
            as_ints(&first.evaluate(EmitTo::All).unwrap()),
            vec![Some(3)]
        );
        let mut last = accumulator(&SparkFirstLast::last(), DataType::Int32, false);
        last.update_batch(&[values], &groups, Some(&mask), 1)
            .unwrap();
        assert_eq!(as_ints(&last.evaluate(EmitTo::All).unwrap()), vec![Some(3)]);
    }

    #[test]
    fn state_and_merge_follow_spark_buffers() {
        for is_first in [true, false] {
            for ignore_nulls in [true, false] {
                let udaf = if is_first {
                    SparkFirstLast::first()
                } else {
                    SparkFirstLast::last()
                };
                let mut left = accumulator(&udaf, DataType::Utf8, ignore_nulls);
                left.update_batch(&[strings(vec![Some("a"), None, None])], &[0, 1, 1], None, 3)
                    .unwrap();
                let left_state = left.state(EmitTo::All).unwrap();
                assert_eq!(
                    as_bools(&left_state[1]),
                    vec![Some(true), Some(!ignore_nulls), Some(false)]
                );

                let mut right = accumulator(&udaf, DataType::Utf8, ignore_nulls);
                right
                    .update_batch(
                        &[strings(vec![Some("b"), Some("c"), Some("d")])],
                        &[0, 1, 2],
                        None,
                        3,
                    )
                    .unwrap();
                let right_state = right.state(EmitTo::All).unwrap();

                let mut merged = accumulator(&udaf, DataType::Utf8, ignore_nulls);
                merged.merge_batch(&left_state, &[0, 1, 2], 3).unwrap();
                merged.merge_batch(&right_state, &[0, 1, 2], 3).unwrap();
                let out = as_strings(&merged.evaluate(EmitTo::All).unwrap());
                let expected: Vec<Option<String>> = match (is_first, ignore_nulls) {
                    (true, true) => vec![Some("a"), Some("c"), Some("d")],
                    (true, false) => vec![Some("a"), None, Some("d")],
                    (false, _) => vec![Some("b"), Some("c"), Some("d")],
                }
                .into_iter()
                .map(|v| v.map(String::from))
                .collect();
                assert_eq!(out, expected, "first={is_first} ignore={ignore_nulls}");
            }
        }
    }

    #[test]
    fn merge_ignores_unset_partial_states() {
        let mut acc = accumulator(&SparkFirstLast::last(), DataType::Int32, false);
        let state: Vec<ArrayRef> = vec![
            ints(vec![Some(1), None, Some(7)]),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        ];
        acc.merge_batch(&state, &[0, 0, 0], 1).unwrap();
        assert_eq!(as_ints(&acc.evaluate(EmitTo::All).unwrap()), vec![Some(1)]);
    }

    #[test]
    fn emit_first_shifts_remaining_groups() {
        for data_type in [DataType::Int32, DataType::Utf8] {
            let mut acc = accumulator(&SparkFirstLast::first(), data_type.clone(), true);
            let input = match data_type {
                DataType::Int32 => ints(vec![Some(0), Some(1), Some(2), None]),
                _ => strings(vec![Some("0"), Some("1"), Some("2"), None]),
            };
            acc.update_batch(&[input], &[0, 1, 2, 3], None, 4).unwrap();
            let size_before = acc.size();
            let head = acc.state(EmitTo::First(2)).unwrap();
            assert_eq!(head[0].len(), 2);
            assert_eq!(as_bools(&head[1]), vec![Some(true), Some(true)]);
            assert!(acc.size() <= size_before);
            let next = match data_type {
                DataType::Int32 => ints(vec![Some(10), Some(11), Some(12)]),
                _ => strings(vec![Some("10"), Some("11"), Some("12")]),
            };
            acc.update_batch(&[next], &[0, 1, 2], None, 3).unwrap();
            let rest = acc.evaluate(EmitTo::All).unwrap();
            let rendered: Vec<Option<String>> = match data_type {
                DataType::Int32 => as_ints(&rest)
                    .into_iter()
                    .map(|v| v.map(|v| v.to_string()))
                    .collect(),
                _ => as_strings(&rest),
            };
            assert_eq!(
                rendered,
                vec![
                    Some("2".to_string()),
                    Some("11".to_string()),
                    Some("12".to_string())
                ]
            );
        }
    }

    #[test]
    fn many_groups_match_reference() {
        let num_rows = 200_000usize;
        let num_groups = 30_000usize;
        let mut seed = 0x2545F4914F6CDD1Du64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let values: Vec<Option<i32>> = (0..num_rows)
            .map(|_| {
                let r = next();
                (r % 4 != 0).then_some((r >> 8) as i32)
            })
            .collect();
        let groups: Vec<usize> = (0..num_rows)
            .map(|_| (next() % num_groups as u64) as usize)
            .collect();
        let mask: Vec<Option<bool>> = (0..num_rows)
            .map(|_| match next() % 5 {
                0 => None,
                1 => Some(false),
                _ => Some(true),
            })
            .collect();

        for is_first in [true, false] {
            for ignore_nulls in [true, false] {
                let udaf = if is_first {
                    SparkFirstLast::first()
                } else {
                    SparkFirstLast::last()
                };
                let expected =
                    reference(is_first, ignore_nulls, &values, &groups, &mask, num_groups);

                let mut partials = Vec::new();
                for chunk in 0..4 {
                    let range = chunk * num_rows / 4..(chunk + 1) * num_rows / 4;
                    let mut acc = accumulator(&udaf, DataType::Int32, ignore_nulls);
                    for batch in range.clone().step_by(8192) {
                        let end = (batch + 8192).min(range.end);
                        acc.update_batch(
                            &[ints(values[batch..end].to_vec())],
                            &groups[batch..end],
                            Some(&filter(mask[batch..end].to_vec())),
                            num_groups,
                        )
                        .unwrap();
                    }
                    partials.push(acc.state(EmitTo::All).unwrap());
                }

                let identity: Vec<usize> = (0..num_groups).collect();
                let mut merged = accumulator(&udaf, DataType::Int32, ignore_nulls);
                for state in &partials {
                    merged.merge_batch(state, &identity, num_groups).unwrap();
                }
                let mut out = as_ints(&merged.state(EmitTo::First(1000)).unwrap()[0]);
                out.extend(as_ints(&merged.evaluate(EmitTo::All).unwrap()));
                assert_eq!(out, expected, "first={is_first} ignore={ignore_nulls}");

                let converted = accumulator(&udaf, DataType::Int32, ignore_nulls)
                    .convert_to_state(&[ints(values.clone())], Some(&filter(mask.clone())))
                    .unwrap();
                let mut from_rows = accumulator(&udaf, DataType::Int32, ignore_nulls);
                from_rows
                    .merge_batch(&converted, &groups, num_groups)
                    .unwrap();
                assert_eq!(
                    as_ints(&from_rows.evaluate(EmitTo::All).unwrap()),
                    expected,
                    "convert_to_state first={is_first} ignore={ignore_nulls}"
                );
            }
        }
    }

    #[test]
    fn typed_outputs_keep_their_data_type() {
        let decimal = DataType::Decimal128(20, 3);
        let mut acc = accumulator(&SparkFirstLast::last(), decimal.clone(), true);
        let input: ArrayRef = Arc::new(
            PrimitiveArray::<Decimal128Type>::from(vec![Some(1234), None])
                .with_data_type(decimal.clone()),
        );
        acc.update_batch(&[input], &[0, 0], None, 1).unwrap();
        let out = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(out.data_type(), &decimal);
        assert_eq!(out.as_primitive::<Decimal128Type>().value(0), 1234);

        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let mut acc = accumulator(&SparkFirstLast::first(), ts.clone(), false);
        let input: ArrayRef = Arc::new(
            PrimitiveArray::<TimestampMicrosecondType>::from(vec![Some(7), Some(8)])
                .with_data_type(ts.clone()),
        );
        acc.update_batch(&[input], &[0, 0], None, 1).unwrap();
        assert_eq!(acc.evaluate(EmitTo::All).unwrap().data_type(), &ts);

        let mut acc = accumulator(&SparkFirstLast::last(), DataType::Boolean, true);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
        acc.update_batch(&[input], &[0, 0, 1], None, 2).unwrap();
        assert_eq!(
            as_bools(&acc.evaluate(EmitTo::All).unwrap()),
            vec![Some(false), None]
        );

        let mut acc = accumulator(&SparkFirstLast::first(), DataType::Int64, false);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        acc.update_batch(&[input], &[1, 1], None, 2).unwrap();
        let out = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            out.as_primitive::<Int64Type>().iter().collect::<Vec<_>>(),
            vec![None, Some(1)]
        );
    }

    #[test]
    fn nested_types_keep_the_row_accumulator() {
        let list = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        assert!(!SparkFirstLast::groups_supported_type(&list));
        assert!(SparkFirstLast::groups_supported_type(&DataType::Utf8));
    }

    #[test]
    fn size_tracks_string_heap() {
        let mut acc = accumulator(&SparkFirstLast::last(), DataType::Utf8, false);
        let long = "x".repeat(1000);
        acc.update_batch(&[strings(vec![Some(&long)])], &[0], None, 1)
            .unwrap();
        assert!(acc.size() >= 1000);
        acc.evaluate(EmitTo::All).unwrap();
        assert!(acc.size() < 1000);
    }
}
