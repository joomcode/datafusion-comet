/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.CometConf

/**
 * Picks the format of each shuffle and broadcast from the engines [[CometExecRule]] chose for the
 * operators on its two sides, without changing any operator. Comet picks a shuffle's format from
 * its producer alone, so a Spark producer gets Comet's columnar shuffle even when its consumer is
 * a Spark operator too, converting rows to Arrow when writing and back to rows when reading. This
 * rule makes that shuffle a Spark shuffle, keeping the inputs of each co-partitioned consumer on
 * one hash function where their keys could hash differently. See [[BoundaryFormats]].
 *
 * It needs the consumers of the boundaries, so [[CometRule]] runs it on whole plans only: the
 * plan without AQE, and the initial plan and each re-optimization under AQE. The Spark shuffles
 * and broadcasts it creates are tagged so that AQE's per-stage conversion keeps them. With
 * `spark.comet.exec.costBasedEngines.enabled` it prices formats like [[CostBasedEngineChoice]],
 * so that it keeps the formats that rule picked.
 */
case class ChooseBoundaryFormats(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!CometConf.COMET_EXEC_BOUNDARY_FORMATS_ENABLED.get(conf) ||
      !CometConf.COMET_EXEC_ENABLED.get(conf)) {
      plan
    } else {
      val pricing =
        if (CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.get(conf)) EngineCostModel(conf)
        else BoundaryFormats.ConversionCount
      BoundaryFormats.applyFormats(plan, pricing)
    }
  }
}
