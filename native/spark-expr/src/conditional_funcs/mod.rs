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

mod case_when;
mod if_expr;
mod logical_short_circuit;
mod null_short_circuit;

pub use case_when::{coerce_to_common_type, create_case_when, create_if_expr, CaseWhenExpr};
pub use if_expr::IfExpr;
pub use logical_short_circuit::LogicalShortCircuit;
pub use null_short_circuit::NullShortCircuit;

/// Whether an expression may be evaluated on rows Spark would skip without changing its value,
/// advancing state or introducing a SQL error. Unknown expression classes are not accepted.
/// SMJ uses this affirmative proof before caching one-side join-filter subexpressions.
pub fn can_evaluate_eagerly(
    expr: &std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    input_schema: &arrow::datatypes::Schema,
) -> bool {
    case_when::cannot_fail(expr, input_schema)
}
