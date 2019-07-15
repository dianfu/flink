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
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.plan.util.RexDefaultVisitor

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable

class PythonScalarFunctionSplitRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc], any),
  "PythonScalarFunctionSplitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val program = calc.getProgram
    program.getProjectList.map(program.expandLocalRef).exists(
      _.accept(new PythonFunctionSplitChecker))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val input = calc.getInput
    val rexBuilder = call.builder().getRexBuilder
    val program = calc.getProgram
    val extractedPythonRexCalls = new mutable.ArrayBuffer[RexCall]()
    val splitter = new PythonFunctionSplitter(
      input.getRowType.getFieldCount - 1, extractedPythonRexCalls)

    val newProjects = program.getProjectList
      .map(program.expandLocalRef)
      .map(_.accept(splitter))

    val newCondition = Option(program.getCondition)
      .map(program.expandLocalRef)
      .map(_.accept(splitter))

    val bottomCalcProjects =
      input.getRowType.getFieldList.indices.map(RexInputRef.of(_, input.getRowType)) ++
      extractedPythonRexCalls
    val bottomCalcFieldNames = SqlValidatorUtil.uniquify(
      input.getRowType.getFieldNames ++
        extractedPythonRexCalls.zipWithIndex.map("f" + _._2),
      SqlValidatorUtil.F_SUGGESTER,
      rexBuilder.getTypeFactory.getTypeSystem.isSchemaCaseSensitive)

    val bottomCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      input,
      RexProgram.create(
        input.getRowType,
        bottomCalcProjects,
        null,
        bottomCalcFieldNames,
        rexBuilder))

    val topCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      bottomCalc,
      RexProgram.create(
        bottomCalc.getRowType,
        newProjects,
        newCondition.orNull,
        calc.getRowType,
        rexBuilder))

    call.transformTo(topCalc)
  }
}

private class PythonFunctionSplitter(
    pythonFunctionOffset: Int,
    extractedPythonRexCalls: mutable.ArrayBuffer[RexCall],
    isChildJavaFunction: Option[Boolean] = None)
  extends RexDefaultVisitor[RexNode] {

  override def visitCall(call: RexCall): RexNode = {
    call.getOperator match {
      case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage ==
        FunctionLanguage.PYTHON =>
        visit(isChildJavaFunction.getOrElse(false), isJavaFunction = false, call)
      case _ =>
        visit(needSplit = false, isJavaFunction = true, call)
    }
  }

  private def visit(needSplit: Boolean, isJavaFunction: Boolean, call: RexCall): RexNode = {
    if (needSplit) {
      extractedPythonRexCalls.append(call)
      new RexInputRef(pythonFunctionOffset + extractedPythonRexCalls.length, call.getType)
    } else {
      call.clone(
        call.getType,
        call.getOperands.asScala.map(_.accept(new PythonFunctionSplitter(
          pythonFunctionOffset, extractedPythonRexCalls, Some(isJavaFunction)))))
    }
  }

  override def visitNode(rexNode: RexNode): RexNode = rexNode
}

private class PythonFunctionSplitChecker(isChildJavaFunction: Option[Boolean] = None)
  extends RexDefaultVisitor[Boolean] {

  override def visitCall(call: RexCall): Boolean = {
    call.getOperator match {
      case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage ==
        FunctionLanguage.PYTHON =>
        visit(isChildJavaFunction.getOrElse(false), isJavaFunction = false, call)
      case _ =>
        visit(needSplit = false, isJavaFunction = true, call)
    }
  }

  override def visitNode(rexNode: RexNode): Boolean = false

  private def visit(needSplit: Boolean, isJavaFunction: Boolean, call: RexCall): Boolean = {
    if (needSplit) {
      true
    } else {
      call.getOperands.asScala
        .exists(_.accept(new PythonFunctionSplitChecker(Some(isJavaFunction))))
    }
  }
}

object PythonScalarFunctionSplitRule {
  val INSTANCE: RelOptRule = new PythonScalarFunctionSplitRule
}
