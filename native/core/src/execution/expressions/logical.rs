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

//! Logical expression builders

use datafusion::physical_expr::expressions::NotExpr;

use crate::unary_expr_builder;

macro_rules! logical_expr_builder {
    ($name:ident, $variant:ident, $is_and:expr) => {
        pub struct $name;
        impl crate::execution::planner::expression_registry::ExpressionBuilder for $name {
            fn build(
                &self,
                expr: &datafusion_comet_proto::spark_expression::Expr,
                schema: arrow::datatypes::SchemaRef,
                planner: &crate::execution::planner::PhysicalPlanner,
            ) -> Result<
                std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                crate::execution::operators::ExecutionError,
            > {
                let value = crate::extract_expr!(expr, $variant);
                let left = planner.create_expr(
                    value.left.as_ref().ok_or_else(|| {
                        crate::execution::operators::ExecutionError::GeneralError(
                            concat!(stringify!($variant), " missing left child").to_string(),
                        )
                    })?,
                    std::sync::Arc::clone(&schema),
                )?;
                let right = planner.create_expr(
                    value.right.as_ref().ok_or_else(|| {
                        crate::execution::operators::ExecutionError::GeneralError(
                            concat!(stringify!($variant), " missing right child").to_string(),
                        )
                    })?,
                    std::sync::Arc::clone(&schema),
                )?;
                datafusion_comet_spark_expr::LogicalShortCircuit::try_new(
                    left, right, $is_and, &schema,
                )
                .map_err(Into::into)
            }
        }
    };
}

/// Macro to define all logical builders at once
macro_rules! define_logical_builders {
    () => {
        logical_expr_builder!(AndBuilder, And, true);
        logical_expr_builder!(OrBuilder, Or, false);
        unary_expr_builder!(NotBuilder, Not, NotExpr::new);
    };
}

define_logical_builders!();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::planner::{expression_registry::ExpressionBuilder, PhysicalPlanner};
    use arrow::datatypes::Schema;
    use datafusion::execution::context::SessionContext;
    use datafusion_comet_proto::spark_expression::{expr::ExprStruct, BinaryExpr, Expr, Literal};
    use std::sync::Arc;

    #[test]
    fn malformed_logical_children_return_errors_instead_of_panicking() {
        let planner = PhysicalPlanner::new(Arc::new(SessionContext::new()), 0);
        let schema = Arc::new(Schema::empty());
        let invalid = Expr::default();
        for is_and in [true, false] {
            // The left child must be valid so the missing-right branch is reached.
            let left = Expr {
                expr_struct: Some(ExprStruct::Literal(Literal {
                    is_null: true,
                    datatype: Some(datafusion_comet_proto::spark_expression::DataType {
                        type_id:
                            datafusion_comet_proto::spark_expression::data_type::DataTypeId::Bool
                                as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            for (left, right, missing) in [
                (None, Some(Box::new(invalid.clone())), "left"),
                (Some(Box::new(left)), None, "right"),
            ] {
                let value = BinaryExpr { left, right };
                let expr = Expr {
                    expr_struct: Some(if is_and {
                        ExprStruct::And(Box::new(value))
                    } else {
                        ExprStruct::Or(Box::new(value))
                    }),
                    ..Default::default()
                };
                let result = if is_and {
                    AndBuilder.build(&expr, Arc::clone(&schema), &planner)
                } else {
                    OrBuilder.build(&expr, Arc::clone(&schema), &planner)
                };
                let error = result.unwrap_err().to_string();
                assert!(
                    error.contains(&format!("missing {missing} child")),
                    "{error}"
                );
            }
        }
    }
}
