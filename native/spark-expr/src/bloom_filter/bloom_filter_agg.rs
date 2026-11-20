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

use arrow::datatypes::{Field, FieldRef};
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use std::sync::Arc;

use crate::bloom_filter::spark_bloom_filter;
use crate::bloom_filter::spark_bloom_filter::{SparkBloomFilter, SparkBloomFilterVersion};

use arrow::array::BinaryArray;
use arrow::array::StringArray;
use arrow::array::{Array, ArrayRef};
use datafusion::common::{downcast_value, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{AggregateUDFImpl, Signature};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::Accumulator;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BloomFilterAgg {
    signature: Signature,
    num_items: i32,
    num_bits: i32,
    /// Output serialization version. Spark <= 4.0 only knows V1; Spark 4.1+'s
    /// `BloomFilter.create` defaults to V2, so the JVM serde sets this to V2 on
    /// 4.1+ to keep `bloom_filter_agg` byte-equivalent with Spark's aggregator.
    version: SparkBloomFilterVersion,
}

#[inline]
fn extract_i32_from_literal(expr: Arc<dyn PhysicalExpr>) -> i32 {
    match (*expr).downcast_ref::<Literal>().unwrap().value() {
        ScalarValue::Int64(scalar_value) => scalar_value.unwrap() as i32,
        _ => {
            unreachable!()
        }
    }
}

impl BloomFilterAgg {
    pub fn new(
        num_items: Arc<dyn PhysicalExpr>,
        num_bits: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        version: SparkBloomFilterVersion,
    ) -> Self {
        assert!(matches!(data_type, DataType::Binary));
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
            num_items: extract_i32_from_literal(num_items),
            num_bits: extract_i32_from_literal(num_bits),
            version,
        }
    }
}

impl AggregateUDFImpl for BloomFilterAgg {
    fn name(&self) -> &str {
        "bloom_filter_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparkBloomFilter::new(
            self.version,
            spark_bloom_filter::optimal_num_hash_functions(self.num_items, self.num_bits),
            self.num_bits,
            // Spark's BloomFilterAggregate always uses BloomFilterImplV2.DEFAULT_SEED (= 0).
            0,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("bits", DataType::Binary, false))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }
}

impl Accumulator for SparkBloomFilter {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        // Hash strings in place rather than materializing a `ScalarValue::Utf8` per row.
        if let Some(strings) = arr.as_any().downcast_ref::<StringArray>() {
            for value in strings.iter().flatten() {
                self.put_binary(value.as_bytes());
            }
            return Ok(());
        }
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;

            match v {
                ScalarValue::Int8(Some(value)) => {
                    self.put_long(value as i64);
                }
                ScalarValue::Int16(Some(value)) => {
                    self.put_long(value as i64);
                }
                ScalarValue::Int32(Some(value)) => {
                    self.put_long(value as i64);
                }
                ScalarValue::Int64(Some(value)) => {
                    self.put_long(value);
                }
                ScalarValue::Utf8(Some(value)) => {
                    self.put_binary(value.as_bytes());
                }
                // Spark's BloomFilterAggregate.update ignores null inputs.
                ScalarValue::Int8(None)
                | ScalarValue::Int16(None)
                | ScalarValue::Int32(None)
                | ScalarValue::Int64(None)
                | ScalarValue::Utf8(None) => {}
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "bloom_filter_agg received an unsupported input type: {other:?}"
                    )));
                }
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Spark's BloomFilterAggregate.eval returns NULL when no bit is set,
        // i.e. the aggregate saw no non-null input. Mirror that here so an
        // empty-input bloom_filter_agg yields NULL rather than a serialized
        // empty filter.
        if self.cardinality() == 0 {
            return Ok(ScalarValue::Binary(None));
        }
        Ok(ScalarValue::Binary(Some(self.spark_serialization())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.heap_size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // There might be a more efficient way to do this by transmuting since calling state() on an
        // Accumulator is considered destructive.
        let state_sv = ScalarValue::Binary(Some(self.state_as_bytes()));
        Ok(vec![state_sv])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(
            states.len(),
            1,
            "Expect one element in 'states' but found {}",
            states.len()
        );
        let state_sv = downcast_value!(states[0], BinaryArray);
        for row in 0..state_sv.len() {
            if state_sv.is_valid(row) {
                self.merge_filter(state_sv.value(row))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spark's BloomFilterAggregate.eval returns NULL when the filter saw no
    /// non-null input (cardinality 0); an untouched accumulator must match.
    #[test]
    fn evaluate_empty_filter_yields_null() {
        let num_bits = 1024;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        let mut acc = SparkBloomFilter::new(SparkBloomFilterVersion::V1, num_hash, num_bits, 0);
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Binary(None));
    }

    /// A filter with at least one set bit serializes to a non-null binary.
    #[test]
    fn evaluate_non_empty_filter_yields_binary() {
        let num_bits = 1024;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        let mut acc = SparkBloomFilter::new(SparkBloomFilterVersion::V1, num_hash, num_bits, 0);
        acc.put_long(42);
        assert!(matches!(
            acc.evaluate().unwrap(),
            ScalarValue::Binary(Some(_))
        ));
    }

    /// `size()` has to include the bit array the filter allocates up front, not only the struct.
    #[test]
    fn size_counts_bit_array() {
        let num_bits = 1 << 20;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        let acc = SparkBloomFilter::new(SparkBloomFilterVersion::V1, num_hash, num_bits, 0);
        assert!(acc.size() >= num_bits as usize / 8);
    }

    #[test]
    fn merge_batch_merges_every_partial_state_of_a_batch() {
        let num_bits = 1024;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        let filter = || SparkBloomFilter::new(SparkBloomFilterVersion::V1, num_hash, num_bits, 0);
        let state = |values: &[i64]| {
            let mut acc = filter();
            for v in values {
                acc.put_long(*v);
            }
            match acc.state().unwrap().remove(0) {
                ScalarValue::Binary(Some(bytes)) => bytes,
                other => panic!("unexpected state {other:?}"),
            }
        };
        let (a, b) = (state(&[1, 2]), state(&[42]));

        let mut separately = filter();
        for s in [&a, &b] {
            let one: ArrayRef = Arc::new(BinaryArray::from(vec![Some(s.as_slice())]));
            separately.merge_batch(&[one]).unwrap();
        }
        let mut together = filter();
        let all: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(a.as_slice()),
            None,
            Some(b.as_slice()),
        ]));
        together.merge_batch(&[all]).unwrap();

        assert_eq!(together.evaluate().unwrap(), separately.evaluate().unwrap());
        assert_ne!(together.evaluate().unwrap(), filter().evaluate().unwrap());
    }

    fn merge_test_filter(version: SparkBloomFilterVersion) -> SparkBloomFilter {
        let num_bits = 1024;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        SparkBloomFilter::new(version, num_hash, num_bits, 0)
    }

    #[test]
    fn merge_batch_uses_the_sliced_row_instead_of_the_prefix_value_buffer() {
        for version in [SparkBloomFilterVersion::V1, SparkBloomFilterVersion::V2] {
            let mut prefix = merge_test_filter(version);
            prefix.put_long(987654321);
            let mut expected = merge_test_filter(version);
            expected.put_long(42);
            let prefix_bytes = prefix.state_as_bytes();
            let expected_bytes = expected.state_as_bytes();
            assert_ne!(
                prefix_bytes, expected_bytes,
                "fixture must distinguish the prefix"
            );
            let values = BinaryArray::from(vec![
                Some(prefix_bytes.as_slice()),
                Some(expected_bytes.as_slice()),
                Some(prefix_bytes.as_slice()),
            ]);
            let sliced: ArrayRef = Arc::new(values.slice(1, 1));
            let mut actual = merge_test_filter(version);
            actual.merge_batch(&[sliced]).unwrap();
            assert_eq!(actual.evaluate().unwrap(), expected.evaluate().unwrap());
        }
    }

    #[test]
    fn merge_batch_handles_multiple_sliced_states_nulls_and_empty_input_for_both_versions() {
        for version in [SparkBloomFilterVersion::V1, SparkBloomFilterVersion::V2] {
            let state = |values: &[i64]| {
                let mut filter = merge_test_filter(version);
                for value in values {
                    filter.put_long(*value);
                }
                filter.state_as_bytes()
            };
            let prefix = state(&[987654321]);
            let a = state(&[1, 2]);
            let b = state(&[42, i64::MIN, i64::MAX]);
            let source = BinaryArray::from(vec![
                Some(prefix.as_slice()),
                Some(a.as_slice()),
                None,
                Some(b.as_slice()),
                Some(prefix.as_slice()),
            ]);
            let mut actual = merge_test_filter(version);
            actual.merge_batch(&[Arc::new(source.slice(1, 3))]).unwrap();
            let mut expected = merge_test_filter(version);
            for value in [1, 2, 42, i64::MIN, i64::MAX] {
                expected.put_long(value);
            }
            assert_eq!(actual.evaluate().unwrap(), expected.evaluate().unwrap());
            let nonempty = actual.evaluate().unwrap();
            let empty: ArrayRef = Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new()));
            let nulls: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>, None]));
            actual.merge_batch(std::slice::from_ref(&empty)).unwrap();
            actual.merge_batch(std::slice::from_ref(&nulls)).unwrap();
            assert_eq!(actual.evaluate().unwrap(), nonempty);
            let mut untouched = merge_test_filter(version);
            untouched.merge_batch(&[empty]).unwrap();
            untouched.merge_batch(&[nulls]).unwrap();
            assert_eq!(untouched.evaluate().unwrap(), ScalarValue::Binary(None));
        }
    }

    #[test]
    fn merge_batch_propagates_a_later_incompatible_state_error() {
        let mut first = merge_test_filter(SparkBloomFilterVersion::V2);
        first.put_long(42);
        let good = first.state_as_bytes();
        let bad = merge_test_filter(SparkBloomFilterVersion::V1).state_as_bytes();
        let input: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(good.as_slice()),
            Some(bad.as_slice()),
        ]));
        let mut actual = merge_test_filter(SparkBloomFilterVersion::V2);
        let error = actual.merge_batch(&[input]).unwrap_err().to_string();
        assert!(error.contains("version mismatch"), "{error}");
    }
}
