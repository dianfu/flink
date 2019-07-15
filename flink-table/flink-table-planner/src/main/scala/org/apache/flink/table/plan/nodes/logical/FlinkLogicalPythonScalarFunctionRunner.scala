/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.RexCall
import org.apache.flink.table.plan.nodes.CommonPythonScalarFunctionRunner

class FlinkLogicalPythonScalarFunctionRunner(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rexCalls: Array[RexCall])
  extends SingleRel(cluster, traitSet, input)
  with CommonPythonScalarFunctionRunner
  with FlinkLogicalRel {

  def getRexCalls: Array[RexCall] = rexCalls

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalPythonScalarFunctionRunner(cluster, traitSet, inputs.get(0), rexCalls)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = mq.getRowCount(child)
    computeSelfCost(rexCalls, planner, rowCnt)
  }

  override def deriveRowType(): RelDataType = getRowType(input, rexCalls)

  override def estimateRowCount(metadata: RelMetadataQuery): Double = {
    metadata.getRowCount(this.getInput)
  }
}
