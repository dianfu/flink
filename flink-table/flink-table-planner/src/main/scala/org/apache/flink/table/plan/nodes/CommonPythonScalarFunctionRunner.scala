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
package org.apache.flink.table.plan.nodes

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexNode, RexProgram}
import org.apache.calcite.sql.validate.SqlValidatorUtil

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait CommonPythonScalarFunctionRunner {

  private[flink] def computeSelfCost(
      rexCalls: Array[RexCall],
      planner: RelOptPlanner,
      rowCnt: Double): RelOptCost = {
    val compCnt = rexCalls.length
    planner.getCostFactory.makeCost(rowCnt, rowCnt * compCnt, 0)
  }

  private[flink] def getRowType(
      input: RelNode,
      rexCalls: Array[RexCall]): RelDataType = {
    val typeFactory = input.getCluster.getTypeFactory

    val fieldTypes =
      input.getRowType.getFieldList.asScala.map(_.getValue) ++ rexCalls.map(_.getType)
    val fieldNames = input.getRowType.getFieldNames ++ rexCalls.zipWithIndex.map("f" + _._2)
    val uniquifiedFieldNames = SqlValidatorUtil.uniquify(
      fieldNames, SqlValidatorUtil.F_SUGGESTER, typeFactory.getTypeSystem.isSchemaCaseSensitive)
    typeFactory.createStructType(fieldTypes, uniquifiedFieldNames)
  }

  private[flink] def callsToString(
      rexCalls: Array[RexCall],
      inputFieldNames: List[String],
      outputFieldNames: List[String],
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    rexCalls
      .map(expression(_, inputFieldNames, None))
      .zip(outputFieldNames).map { case (e, o) =>
        if (e != o) {
          e + " AS " + o
        } else {
          e
        }
      }.mkString(", ")
  }
}
