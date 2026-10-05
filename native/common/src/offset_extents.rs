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

//! How much of each 32-bit offset buffer (`Utf8`, `Binary`, `List` and `Map` at any depth) an
//! array addresses, so that `concat` or a repeating `take` can be fed few enough rows that no
//! offset buffer of the result exceeds `i32::MAX`. Nodes are visited in a pre-order fixed by
//! the data type, so the extents of two arrays of one type line up position by position.

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;

/// The largest extent a 32-bit offset buffer can address.
pub const MAX_OFFSET_EXTENT: usize = i32::MAX as usize;

/// The extent of every 32-bit offset node of `array`, in pre-order. Dictionary values are
/// included whole, since concatenating dictionaries can concatenate their values.
pub fn offset_extents(array: &dyn Array, extents: &mut Vec<usize>) {
    visit(array, &[0, array.len()], true, &mut |bounds| {
        extents.push(bounds[1] - bounds[0])
    });
}

/// Raises `row_extents[i]` to the largest extent row `i` of `array` has in any of its 32-bit
/// offset nodes. Repeating row `i` `n` times adds at most `n * row_extents[i]` to every such
/// node. Dictionary values are skipped: repeating keys does not copy them.
pub fn max_row_offset_extents(array: &dyn Array, row_extents: &mut [usize]) {
    assert_eq!(array.len(), row_extents.len());
    let rows: Vec<usize> = (0..=array.len()).collect();
    visit(array, &rows, false, &mut |bounds| {
        for (row, extent) in row_extents.iter_mut().enumerate() {
            *extent = (*extent).max(bounds[row + 1] - bounds[row]);
        }
    });
}

/// Accumulates the extents of the batches that would be concatenated, to tell whether one more
/// keeps every offset node within [`MAX_OFFSET_EXTENT`].
#[derive(Debug)]
pub struct OffsetBudget {
    limit: usize,
    used: Vec<usize>,
    scratch: Vec<usize>,
}

impl Default for OffsetBudget {
    fn default() -> Self {
        Self::with_limit(MAX_OFFSET_EXTENT)
    }
}

impl OffsetBudget {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            limit,
            used: vec![],
            scratch: vec![],
        }
    }

    /// Adds the columns' extents if they fit beside those already added, and reports whether
    /// they did. An empty budget accepts anything.
    pub fn try_add(&mut self, columns: &[impl AsRef<dyn Array>]) -> bool {
        self.scratch.clear();
        for column in columns {
            offset_extents(column.as_ref(), &mut self.scratch);
        }
        if self.used.is_empty() {
            std::mem::swap(&mut self.used, &mut self.scratch);
            return true;
        }
        if self.used.len() != self.scratch.len()
            || self
                .used
                .iter()
                .zip(&self.scratch)
                .any(|(used, extent)| used + extent > self.limit)
        {
            return false;
        }
        for (used, extent) in self.used.iter_mut().zip(&self.scratch) {
            *used += extent;
        }
        true
    }

    pub fn clear(&mut self) {
        self.used.clear();
    }
}

/// Calls `f` with the bounds, in the node's own index space, that `bounds` of `array` cover in
/// every 32-bit offset node below it.
fn visit(array: &dyn Array, bounds: &[usize], dictionaries: bool, f: &mut dyn FnMut(&[usize])) {
    let through =
        |offsets: &[i32]| -> Vec<usize> { bounds.iter().map(|&b| offsets[b] as usize).collect() };
    match array.data_type() {
        DataType::Utf8 => f(&through(array.as_string::<i32>().value_offsets())),
        DataType::Binary => f(&through(array.as_binary::<i32>().value_offsets())),
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let child = through(list.value_offsets());
            f(&child);
            visit(list.values().as_ref(), &child, dictionaries, f);
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let child = through(map.value_offsets());
            f(&child);
            visit(map.entries(), &child, dictionaries, f);
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let child: Vec<usize> = bounds.iter().map(|&b| offsets[b] as usize).collect();
            visit(list.values().as_ref(), &child, dictionaries, f);
        }
        DataType::FixedSizeList(_, size) => {
            let size = *size as usize;
            let child: Vec<usize> = bounds.iter().map(|&b| b * size).collect();
            visit(
                array.as_fixed_size_list().values().as_ref(),
                &child,
                dictionaries,
                f,
            );
        }
        DataType::Struct(_) => {
            for column in array.as_struct().columns() {
                visit(column.as_ref(), bounds, dictionaries, f);
            }
        }
        DataType::Dictionary(_, _) if dictionaries => {
            let values = array.as_any_dictionary().values();
            visit(values.as_ref(), &[0, values.len()], dictionaries, f);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, Int32Type};
    use std::sync::Arc;

    fn nested() -> ArrayRef {
        let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "bb", "", "cccc", "dd"]));
        let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
        ]);
        let element = StructArray::new(fields.clone(), vec![names, ids], None);
        Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Struct(fields), true)),
            OffsetBuffer::new(vec![0, 2, 2, 5].into()),
            Arc::new(element),
            None,
        ))
    }

    #[test]
    fn extents_cover_every_offset_node_of_a_sliced_nested_array() {
        let mut extents = vec![];
        offset_extents(nested().as_ref(), &mut extents);
        assert_eq!(extents, vec![5, 9]);

        let mut extents = vec![];
        offset_extents(nested().slice(1, 2).as_ref(), &mut extents);
        assert_eq!(extents, vec![3, 6]);
    }

    #[test]
    fn row_extents_take_the_largest_node_per_row() {
        let mut rows = vec![0; 3];
        max_row_offset_extents(nested().as_ref(), &mut rows);
        assert_eq!(rows, vec![3, 0, 6]);

        let mut rows = vec![0; 2];
        max_row_offset_extents(nested().slice(1, 2).as_ref(), &mut rows);
        assert_eq!(rows, vec![0, 6]);
    }

    #[test]
    fn maps_and_dictionaries() {
        let values = Int32Array::from(vec![1, 2]);
        let map =
            MapArray::new_from_strings(["k1", "k22"].into_iter(), &values, &[0, 1, 2]).unwrap();
        let mut extents = vec![];
        offset_extents(&map, &mut extents);
        assert_eq!(extents, vec![2, 5]);

        let dict: DictionaryArray<Int32Type> = vec!["xyz", "xyz", "q"].into_iter().collect();
        let mut extents = vec![];
        offset_extents(&dict, &mut extents);
        assert_eq!(extents, vec![4]);
        let mut rows = vec![0; 3];
        max_row_offset_extents(&dict, &mut rows);
        assert_eq!(rows, vec![0, 0, 0]);
    }

    #[test]
    fn budget_refuses_what_would_overflow_and_restarts_when_cleared() {
        let column = |s: &str| -> Vec<ArrayRef> { vec![Arc::new(StringArray::from(vec![s]))] };
        let mut budget = OffsetBudget::with_limit(10);
        assert!(budget.try_add(&column("aaaaaa")));
        assert!(budget.try_add(&column("aaaa")));
        assert!(!budget.try_add(&column("a")));
        budget.clear();
        assert!(budget.try_add(&column("aaaaaaaaaaaaaaa")));
    }
}
