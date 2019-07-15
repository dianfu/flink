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

package org.apache.flink.table.plan.nodes.datastream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.api.{StreamQueryConfig, TableConfig}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.plan.nodes.CommonPythonScalarFunctionRunner
import org.apache.flink.table.plan.nodes.datastream.DataStreamPythonScalarFunctionRunner._
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.python.{PythonFunction, PythonFunctionInfo}
import org.apache.flink.table.runtime.CRowProcessRunner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class DataStreamPythonScalarFunctionRunner(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rexCalls: Array[RexCall],
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with CommonPythonScalarFunctionRunner
  with DataStreamRel {

  val inputSchema = new RowSchema(input.getRowType)
  val resultSchema = new RowSchema(deriveRowType())

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamPythonScalarFunctionRunner(
      cluster, traitSet, inputs.get(0), rexCalls, ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("calls", callsToString(
        rexCalls,
        input.getRowType.getFieldNames.toList,
        resultSchema.fieldNames.subList(input.getRowType.getFieldCount, resultSchema.arity).toList,
        getExpressionString))
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

  private lazy val (pythonFunctionInputNodes, pythonFunctionInfos)
    : (Array[RexNode], Array[PythonFunctionInfo]) = {
    val inputNodes = new mutable.ArrayBuffer[RexNode]()
    val pythonFunctionInfos = rexCalls.map(createPythonScalarFunctionInfo(_, inputNodes))
    (inputNodes.toArray, pythonFunctionInfos)
  }

  private def createPythonScalarFunctionInfo(
      rexCall: RexCall,
      inputNodes: mutable.ArrayBuffer[RexNode]): PythonFunctionInfo = rexCall.getOperator match {
    case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage == FunctionLanguage.PYTHON =>
      val inputs = new mutable.ArrayBuffer[AnyRef]()
      rexCall.getOperands.foreach {
        case argCall: RexCall if argCall.getOperator.asInstanceOf[ScalarSqlFunction]
          .getScalarFunction.getLanguage == FunctionLanguage.PYTHON =>
          val argPythonInfo = createPythonScalarFunctionInfo(argCall, inputNodes)
          inputs.append(argPythonInfo)

        case argNode: RexNode =>
          inputs.append(Integer.valueOf(inputNodes.length))
          inputNodes.append(argNode)
      }
      val inputTypes = rexCall.getOperands.map(
        operand => TypeConversions.fromLegacyInfoToDataType(
          FlinkTypeFactory.toTypeInfo(operand.getType)))
      val resultType = TypeConversions.fromLegacyInfoToDataType(
        FlinkTypeFactory.toTypeInfo(rexCall.getType))
      new PythonFunctionInfo(
        sfc.getScalarFunction.asInstanceOf[PythonFunction],
        inputs,
        inputTypes,
        resultType)
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {
    val config = planner.getConfig

    val inputDataStream =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(planner, queryConfig)

    val inputParallelism = inputDataStream.getParallelism

    val processFunctionResultTypeInfo = new RowTypeInfo(
      inputSchema.fieldTypeInfos ++
        pythonFunctionInputNodes.map(node => FlinkTypeFactory.toTypeInfo(node.getType)): _*)

    val genProcessFunction = generateProcessFunction(
      ruleDescription,
      pythonFunctionInputNodes,
      processFunctionResultTypeInfo,
      config)

    val processFunction = new CRowProcessRunner(
      genProcessFunction.name,
      genProcessFunction.code,
      CRowTypeInfo(processFunctionResultTypeInfo))

    val pythonScalarFunctionOperator = createPythonScalarFunctionOperator

    inputDataStream
      .process(processFunction)
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputParallelism)
      .transform(
        "PythonScalarFunctionRunner",
        CRowTypeInfo(resultSchema.typeInfo),
        pythonScalarFunctionOperator)
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputParallelism)
  }

  private[flink] def generateProcessFunction(
      ruleDescription: String,
      rexNodes: Seq[RexNode],
      resultTypeInfo: RowTypeInfo,
      config: TableConfig): GeneratedFunction[ProcessFunction[_, _], Row] = {
    val generator = new FunctionCodeGenerator(config, false, inputSchema.typeInfo)

    val projectedType = new RowTypeInfo(
      rexNodes.map(node => FlinkTypeFactory.toTypeInfo(node.getType)): _*)

    val projection = generator.generateResultExpression(
      projectedType,
      projectedType.getFieldNames,
      rexNodes)

    val body =
      s"""
         |${projection.code}
         |${generator.collectorTerm}.collect(
         |  ${classOf[Row].getCanonicalName}.join(
         |    ${generator.input1Term}, ${projection.resultTerm}));
         |""".stripMargin

    generator.generateFunction(
      ruleDescription,
      classOf[ProcessFunction[_, _]],
      body,
      resultTypeInfo)
  }

  private[flink] def createPythonScalarFunctionOperator: OneInputStreamOperator[CRow, CRow] = {
    val clazz = Class.forName(PYTHON_SCALAR_FUNCTION_OPERATOR_NAME)
    val ctor = clazz.getConstructor(classOf[util.List[_]], classOf[Int])
    ctor.newInstance(pythonFunctionInfos.toList.asJava, Integer.valueOf(inputSchema.arity))
      .asInstanceOf[OneInputStreamOperator[CRow, CRow]]
  }
}

object DataStreamPythonScalarFunctionRunner {
  val PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
    "org.apache.flink.table.runtime.operators.python.PythonScalarFunctionOperator"
}
