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

package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexProgram}
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalPythonScalarFunctionRunner}
import org.apache.flink.table.plan.util.RexDefaultVisitor

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class LogicalCalcToLogicalPythonScalarFunctionRunnerRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc], any),
  "LogicalCalcToLogicalPythonScalarFunctionRunnerRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val program = calc.getProgram
    program.getExprList.asScala.toArray.exists(_.accept(PythonFunctionFinder))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val program = calc.getProgram
    val extractedPythonRexCalls = new mutable.ArrayBuffer[RexCall]()
    val extractor = new PythonFunctionExtractor(
      calc.getInput.getRowType.getFieldCount - 1, extractedPythonRexCalls)

    val newProjects = program.getProjectList
      .map(program.expandLocalRef)
      .map(_.accept(extractor))

    val newCondition = Option(program.getCondition)
      .map(program.expandLocalRef)
      .map(_.accept(extractor))

    val pythonFunctionRunner = new FlinkLogicalPythonScalarFunctionRunner(
      calc.getCluster,
      calc.getTraitSet,
      calc.getInput,
      extractedPythonRexCalls.toArray)

    val newCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      pythonFunctionRunner,
      RexProgram.create(
        pythonFunctionRunner.getRowType,
        newProjects,
        newCondition.orNull,
        calc.getRowType,
        call.builder.getRexBuilder))

    call.transformTo(newCalc)
  }
}

private class PythonFunctionExtractor(
    pythonFunctionOffset: Int,
    extractedPythonRexCalls: mutable.ArrayBuffer[RexCall])
  extends RexDefaultVisitor[RexNode] {

  override def visitCall(call: RexCall): RexNode = {
    call.getOperator match {
      case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage ==
        FunctionLanguage.PYTHON =>
        extractedPythonRexCalls.append(call)
        new RexInputRef(pythonFunctionOffset + extractedPythonRexCalls.length, call.getType)
      case _ =>
        call.clone(
          call.getType,
          call.getOperands.asScala.map(_.accept(new PythonFunctionExtractor(
            pythonFunctionOffset, extractedPythonRexCalls))))
    }
  }

  override def visitNode(rexNode: RexNode): RexNode = rexNode
}

/**
  * Checks whether there is a Python ScalarFunction in a RexNode.
  */
object PythonFunctionFinder extends RexDefaultVisitor[Boolean] {

  override def visitCall(call: RexCall): Boolean = {
    call.getOperator match {
      case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage ==
        FunctionLanguage.PYTHON => true
      case _ =>
        call.getOperands.exists(_.accept(this))
    }
  }

  override def visitNode(rexNode: RexNode): Boolean = false
}

object LogicalCalcToLogicalPythonScalarFunctionRunnerRule {
  val INSTANCE: RelOptRule = new LogicalCalcToLogicalPythonScalarFunctionRunnerRule
}
