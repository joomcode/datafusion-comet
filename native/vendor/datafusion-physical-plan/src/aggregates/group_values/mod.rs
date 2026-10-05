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

//! [`GroupValues`] trait for storing and interning group keys

use arrow::array::types::{
    Date32Type, Date64Type, Decimal128Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::array::{Array, ArrayRef, AsArray, downcast_primitive};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion_common::Result;

use datafusion_expr::EmitTo;

pub mod multi_group_by;

mod row;
pub use row::GroupValuesRows;
mod single_group_by;
use datafusion_physical_expr::binary_map::OutputType;
use multi_group_by::GroupValuesColumn;

pub(crate) use single_group_by::primitive::HashValue;

use crate::aggregates::{
    group_values::single_group_by::{
        boolean::GroupValuesBoolean, bytes::GroupValuesBytes,
        bytes_view::GroupValuesBytesView, primitive::GroupValuesPrimitive,
    },
    order::GroupOrdering,
};

mod metrics;
mod null_builder;

pub(crate) use metrics::GroupByMetrics;

/// Stores the group values during hash aggregation.
///
/// # Background
///
/// In a query such as `SELECT a, b, count(*) FROM t GROUP BY a, b`, the group values
/// identify each group, and correspond to all the distinct values of `(a,b)`.
///
/// ```sql
/// -- Input has 4 rows with 3 distinct combinations of (a,b) ("groups")
/// create table t(a int, b varchar)
/// as values (1, 'a'), (2, 'b'), (1, 'a'), (3, 'c');
///
/// select a, b, count(*) from t group by a, b;
/// ----
/// 1 a 2
/// 2 b 1
/// 3 c 1
/// ```
///
/// # Design
///
/// Managing group values is a performance critical operation in hash
/// aggregation. The major operations are:
///
/// 1. Intern: Quickly finding existing and adding new group values
/// 2. Emit: Returning the group values as an array
///
/// There are multiple specialized implementations of this trait optimized for
/// different data types and number of columns, optimized for these operations.
/// See [`new_group_values`] for details.
///
/// # Group Ids
///
/// Each distinct group in a hash aggregation is identified by a unique group id
/// (usize) which is assigned by instances of this trait. Group ids are
/// continuous without gaps, starting from 0.
pub trait GroupValues: Send {
    /// Calculates the group id for each input row of `cols`, assigning new
    /// group ids as necessary.
    ///
    /// When the function returns, `groups`  must contain the group id for each
    /// row in `cols`.
    ///
    /// If a row has the same value as a previous row, the same group id is
    /// assigned. If a row has a new value, the next available group id is
    /// assigned.
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()>;

    /// Returns the number of bytes of memory used by this [`GroupValues`].
    ///
    /// May be expensive; check the implementation before calling on hot paths.
    fn size(&self) -> usize;

    /// Returns true if this [`GroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values (distinct group values) stored in this [`GroupValues`]
    fn len(&self) -> usize;

    /// Emits the group values
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, num_rows: usize);

    /// Returns false if interning `cols` could take a 32-bit offset buffer of the stored
    /// group values, or of the arrays they are emitted as, past `i32::MAX`.
    fn has_offset_room(&self, _cols: &[ArrayRef]) -> bool {
        true
    }
}

/// The largest extent `array` addresses in any of its 32-bit offset buffers (`Utf8`,
/// `Binary`, `List` and `Map` at any depth): bytes for the former, entries for the latter.
pub(crate) fn max_offset_extent(array: &dyn Array) -> usize {
    max_extent(array, 0, array.len())
}

fn max_extent(array: &dyn Array, start: usize, end: usize) -> usize {
    match array.data_type() {
        DataType::Utf8 => {
            let offsets = array.as_string::<i32>().value_offsets();
            (offsets[end] - offsets[start]) as usize
        }
        DataType::Binary => {
            let offsets = array.as_binary::<i32>().value_offsets();
            (offsets[end] - offsets[start]) as usize
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let (start, end) = (offsets[start] as usize, offsets[end] as usize);
            (end - start).max(max_extent(list.values().as_ref(), start, end))
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let (start, end) = (offsets[start] as usize, offsets[end] as usize);
            (end - start).max(max_extent(map.entries(), start, end))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            max_extent(
                list.values().as_ref(),
                offsets[start] as usize,
                offsets[end] as usize,
            )
        }
        DataType::FixedSizeList(_, size) => {
            let size = *size as usize;
            max_extent(
                array.as_fixed_size_list().values().as_ref(),
                start * size,
                end * size,
            )
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .map(|column| max_extent(column.as_ref(), start, end))
            .max()
            .unwrap_or(0),
        DataType::Dictionary(_, _) => {
            let values = array.as_any_dictionary().values();
            max_extent(values.as_ref(), 0, values.len()).saturating_mul(end - start)
        }
        _ => 0,
    }
}

/// True if `data_type` has a 32-bit offset buffer at any depth.
pub(crate) fn has_offset_buffer(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::Binary | DataType::List(_) | DataType::Map(_, _) => {
            true
        }
        DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => has_offset_buffer(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| has_offset_buffer(field.data_type())),
        DataType::Dictionary(_, values) => has_offset_buffer(values),
        DataType::RunEndEncoded(_, values) => has_offset_buffer(values.data_type()),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| has_offset_buffer(field.data_type())),
        _ => false,
    }
}

/// Whether encoded group rows of `held_bytes`, after appending rows decoded from `cols`,
/// still decode into arrays whose 32-bit offsets fit. Every row-format encoding takes at least
/// one byte per value byte and per list entry, so the decoded extent of each offset buffer is
/// at most the encoded size.
pub(crate) fn rows_have_offset_room(held_bytes: usize, cols: &[ArrayRef]) -> bool {
    let incoming = cols
        .iter()
        .map(|col| max_offset_extent(col.as_ref()))
        .fold(0usize, usize::saturating_add);
    held_bytes.saturating_add(incoming) <= i32::MAX as usize
}

/// Return a specialized implementation of [`GroupValues`] for the given schema.
///
/// [`GroupValues`] implementations choosing logic:
///
///   - If group by single column, and type of this column has
///     the specific [`GroupValues`] implementation, such implementation
///     will be chosen.
///
///   - If group by multiple columns, and all column types have the specific
///     `GroupColumn` implementations, `GroupValuesColumn` will be chosen.
///
///   - Otherwise, the general implementation `GroupValuesRows` will be chosen.
///
/// `GroupColumn`:  crate::aggregates::group_values::multi_group_by::GroupColumn
/// `GroupValuesColumn`: crate::aggregates::group_values::multi_group_by::GroupValuesColumn
/// `GroupValuesRows`: crate::aggregates::group_values::GroupValuesRows
pub fn new_group_values(
    schema: SchemaRef,
    group_ordering: &GroupOrdering,
) -> Result<Box<dyn GroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();

        macro_rules! downcast_helper {
            ($t:ty, $d:ident) => {
                return Ok(Box::new(GroupValuesPrimitive::<$t>::new($d.clone())))
            };
        }

        downcast_primitive! {
            d => (downcast_helper, d),
            _ => {}
        }

        match d {
            DataType::Date32 => {
                downcast_helper!(Date32Type, d);
            }
            DataType::Date64 => {
                downcast_helper!(Date64Type, d);
            }
            DataType::Time32(t) => match t {
                TimeUnit::Second => downcast_helper!(Time32SecondType, d),
                TimeUnit::Millisecond => downcast_helper!(Time32MillisecondType, d),
                _ => {}
            },
            DataType::Time64(t) => match t {
                TimeUnit::Microsecond => downcast_helper!(Time64MicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(Time64NanosecondType, d),
                _ => {}
            },
            DataType::Timestamp(t, _tz) => match t {
                TimeUnit::Second => downcast_helper!(TimestampSecondType, d),
                TimeUnit::Millisecond => downcast_helper!(TimestampMillisecondType, d),
                TimeUnit::Microsecond => downcast_helper!(TimestampMicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(TimestampNanosecondType, d),
            },
            DataType::Decimal128(_, _) => {
                downcast_helper!(Decimal128Type, d);
            }
            DataType::Utf8 => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Utf8)));
            }
            DataType::LargeUtf8 => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Utf8)));
            }
            DataType::Utf8View => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::Utf8View)));
            }
            DataType::Binary => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Binary)));
            }
            DataType::LargeBinary => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Binary)));
            }
            DataType::BinaryView => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::BinaryView)));
            }
            DataType::Boolean => {
                return Ok(Box::new(GroupValuesBoolean::new()));
            }
            _ => {}
        }
    }

    if multi_group_by::supported_schema(schema.as_ref()) {
        if matches!(group_ordering, GroupOrdering::None) {
            Ok(Box::new(GroupValuesColumn::<false>::try_new(schema)?))
        } else {
            Ok(Box::new(GroupValuesColumn::<true>::try_new(schema)?))
        }
    } else {
        Ok(Box::new(GroupValuesRows::try_new(schema)?))
    }
}
