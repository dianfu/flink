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

package org.apache.flink.table.codegen

import java.math.{BigDecimal => JBigDecimal}
import java.util

import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{FINAL, FIRST, LAST, NEXT, PREV, RUNNING}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForTypeInfo, newName, primitiveDefaultValue}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
  * A code generator for generating CEP related functions.
  *
  * @param config configuration that determines runtime behavior
  * @param input type information about the first input of the Function
  * @param patternNames the names of patterns
  * @param generateCondition whether the code generator is generating [[IterativeCondition]]
  * @param patternName the name of current pattern
  */
class MatchCodeGenerator(
    config: TableConfig,
    input: TypeInformation[_ <: Any],
    patternNames: Seq[String],
    generateCondition: Boolean,
    patternName: Option[String] = None)
  extends CodeGenerator(config, false, input){

  /**
    * @return term of flag indicating RUNNING semantics is desired
    */
  private var runningTerm = newName("running")

  /**
    * @return term of pattern names
    */
  private var patternNameListTerm = newName("patternNameList")

  /**
    * @return term of current pattern which is processing
    */
  private var currPatternTerm = newName("currPattern")

  /**
    * @return term of current event which is processing
    */
  private var currEventTerm = newName("currEvent")

  private def buildPatternNameList(): String = {
    for (patternName <- patternNames) yield
      s"""
        |$patternNameListTerm.add("$patternName");
        |""".stripMargin
  }.mkString("\n")

  /**
    * Generates a [[IterativeCondition]] that can be passed to Java compiler.
    *
    * @param name Class name of the iterative condition. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the iterative condition method
    * @return instance of GeneratedIterativeCondition
    */
  def generateIterativeCondition(
      name: String,
      bodyCode: String)
    : GeneratedIterativeCondition = {

    val funcName = newName(name)
    val inputTypeTerm = boxedTypeTermForTypeInfo(input)
    val rowTypeTerm = classOf[Row].getCanonicalName

    val funcCode = j"""
      public class $funcName
          extends ${classOf[IterativeCondition[_]].getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public boolean filter(
          Object _in1, ${classOf[IterativeCondition.Context[_]].getCanonicalName} $contextTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          $rowTypeTerm $currEventTerm = null;
          String $currPatternTerm = null;
          boolean $runningTerm = true;
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin

    GeneratedIterativeCondition(funcName, funcCode)
  }

  def generatePatternSelectFunction(
      name: String,
      bodyCode: String)
    : GeneratedPatternSelectFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[Row]]].getCanonicalName
    val rowTypeTerm = classOf[Row].getCanonicalName
    val patternNameListTypeTerm = classOf[java.util.List[java.lang.String]].getCanonicalName

    val funcCode = j"""
      public class $funcName
          implements ${classOf[PatternSelectFunction[_, _]].getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public Object select(java.util.Map<String, java.util.List<Object>> _in1)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          $rowTypeTerm $currEventTerm = null;
          $patternNameListTypeTerm $patternNameListTerm = new java.util.ArrayList();
          String $currPatternTerm = null;
          boolean $runningTerm = true;
          ${buildPatternNameList()}
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin

    GeneratedPatternSelectFunction(funcName, funcCode)
  }

  def generatePatternFlatSelectFunction(
      name: String,
      bodyCode: String)
    : GeneratedPatternFlatSelectFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[Row]]].getCanonicalName
    val rowTypeTerm = classOf[Row].getCanonicalName
    val patternNameListTypeTerm = classOf[java.util.List[java.lang.String]].getCanonicalName

    val funcCode = j"""
      public class $funcName
          implements ${classOf[PatternFlatSelectFunction[_, _]].getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public void flatSelect(java.util.Map<String, java.util.List<Object>> _in1,
            org.apache.flink.util.Collector $collectorTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          $rowTypeTerm $currEventTerm = null;
          $patternNameListTypeTerm $patternNameListTerm = new java.util.ArrayList();
          String $currPatternTerm = null;
          boolean $runningTerm = false;
          ${buildPatternNameList()}
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin

    GeneratedPatternFlatSelectFunction(funcName, funcCode)
  }

  def generateFlatSelectOutputExpression(
      measures: util.Map[String, RexNode],
      inputTypeInfo: TypeInformation[_],
      returnType: RowSchema)
    : GeneratedExpression = {

    val nullTerm = newName("isNull")
    val patternNameTerm = newName("patternName")
    val eventNameTerm = newName("event")
    val rowTypeTerm = classOf[Row].getCanonicalName
    val listRowTypeTerm = classOf[java.util.List[Row]].getCanonicalName

    val fieldExprs = generateFieldsAccess(inputTypeInfo, eventNameTerm) ++
      returnType.physicalFieldNames
        .filter(measures.containsKey(_))
        .map(fieldName => generateExpression(measures.get(fieldName)))
    val resultExpression = generateResultExpression(
      fieldExprs,
      returnType.physicalTypeInfo,
      returnType.physicalFieldNames)

    val resultCode =
      s"""
        |for (String $patternNameTerm : $patternNameListTerm) {
        |  $currPatternTerm = $patternNameTerm;
        |  for ($rowTypeTerm $eventNameTerm : ($listRowTypeTerm)
        |       $input1Term.get($patternNameTerm)) {
        |    $currEventTerm = $eventNameTerm;
        |    ${resultExpression.code}
        |    $collectorTerm.collect(${resultExpression.resultTerm});
        |  }
        |}
        |$currPatternTerm = null;
        |$currEventTerm = null;
        |boolean $nullTerm = false;
        |""".stripMargin

    GeneratedExpression("", nullTerm, resultCode, null)
  }

  override def visitCall(call: RexCall): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toTypeInfo(call.getType)
    call.getOperator match {
      case PREV =>
        generatePrev(
          call.operands.get(0).asInstanceOf[RexInputRef],
          call.operands.get(1).asInstanceOf[RexLiteral],
          resultType)

      case NEXT =>
        throw new CodeGenException(s"Unsupported call: $call")

      case FIRST | LAST =>
        val countLiteral = call.operands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generateFirstLast(
          call.operands.get(0),
          count,
          resultType,
          running = true,
          call.getOperator == FIRST)

      case RUNNING | FINAL =>
        generateRunningFinal(
          call.operands.get(0),
          resultType,
          call.getOperator == RUNNING)

      case _ => super.visitCall(call)
    }
  }

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val patternNameTerm = newName("patternName")
    val eventNameTerm = newName("event")
    val resultType = new ListTypeInfo(FlinkTypeFactory.toTypeInfo(fieldRef.getType))

    val rowTypeTerm = classOf[Row].getCanonicalName
    val listRowTypeTerm = classOf[java.util.List[Row]].getCanonicalName
    val iterRowTypeTerm = classOf[java.lang.Iterable[Row]].getCanonicalName

    val resultCode =
      if (generateCondition) {
        s"""
          |boolean $nullTerm;
          |java.util.List $resultTerm = new java.util.ArrayList();
          |for ($rowTypeTerm $eventNameTerm : ($iterRowTypeTerm)
          |    $contextTerm.getEventsForPattern("${fieldRef.getAlpha}")) {
          |  $resultTerm.add($eventNameTerm.getField(${fieldRef.getIndex}));
          |  if ($runningTerm && $eventNameTerm == $currEventTerm) {
          |    break;
          |  }
          |}
          |$nullTerm = false;
          |""".stripMargin
      } else {
        s"""
          |boolean $nullTerm;
          |java.util.List $resultTerm = new java.util.ArrayList();
          |for (String $patternNameTerm : $patternNameListTerm) {
          |  if ($patternNameTerm.equals("${fieldRef.getAlpha}") ||
          |      ${fieldRef.getAlpha.equals("*")}) {
          |    boolean skipLoop = false;
          |    for ($rowTypeTerm $eventNameTerm : ($listRowTypeTerm)
          |        $input1Term.get($patternNameTerm)) {
          |      $resultTerm.add($eventNameTerm.getField(${fieldRef.getIndex}));
          |      if ($runningTerm && $eventNameTerm == $currEventTerm) {
          |        skipLoop = true;
          |        break;
          |      }
          |    }
          |    if (skipLoop) {
          |      break;
          |    }
          |  }
          |
          |  if ($runningTerm && $currPatternTerm.equals($patternNameTerm)) {
          |    break;
          |  }
          |}
          |$nullTerm = false;
          |""".stripMargin
      }
    GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)
  }

  private def generatePrev(
      rexNode: RexNode,
      countLiteral: RexLiteral,
      resultType: TypeInformation[_])
    : GeneratedExpression = {
    val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()

    rexNode match {
      case patternFieldRef: RexPatternFieldRef =>
        if (count == 0 && patternFieldRef.getAlpha == patternName.get) {
          // return current one
          return visitInputRef(patternFieldRef)
        }

        val listName = newName("patternEvents")
        val resultTerm = newName("result")
        val nullTerm = newName("isNull")
        val indexTerm = newName("eventIndex")
        val visitedEventNumberTerm = newName("visitedEventNumber")
        val eventTerm = newName("event")
        val resultTypeTerm = boxedTypeTermForTypeInfo(resultType)
        val defaultValue = primitiveDefaultValue(resultType)

        val rowTypeTerm = classOf[Row].getCanonicalName

        val patternNamesToVisit = patternNames
          .take(patternNames.indexOf(patternFieldRef.getAlpha) + 1)
          .reverse
        val findEvent: String = {
          for (patternName1 <- patternNamesToVisit) yield
            s"""
              |for ($rowTypeTerm $eventTerm : $contextTerm.getEventsForPattern("$patternName1")) {
              |  $listName.add($eventTerm.getField(${patternFieldRef.getIndex}));
              |}
              |
              |$indexTerm = $listName.size() - ($count - $visitedEventNumberTerm);
              |if ($indexTerm >= 0) {
              |  $resultTerm = ($resultTypeTerm) $listName.get($indexTerm);
              |  $nullTerm = false;
              |  break;
              |}
              |
              |$visitedEventNumberTerm += $listName.size();
              |$listName.clear();
              |""".stripMargin
        }.mkString("\n")

        val resultCode =
          s"""
            |int $visitedEventNumberTerm = 0;
            |int $indexTerm;
            |java.util.List $listName = new java.util.ArrayList();
            |$resultTypeTerm $resultTerm = $defaultValue;
            |boolean $nullTerm = true;
            |do {
            |  $findEvent
            |} while (false);
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)
    }
  }

  private def generateFirstLast(
      rexNode: RexNode,
      count: Int,
      resultType: TypeInformation[_],
      running: Boolean,
      first: Boolean)
    : GeneratedExpression = {
    rexNode match {
      case _: RexPatternFieldRef =>
        val generatedExpression = generateExpression(rexNode)

        val eventsName = generatedExpression.resultTerm
        val resultTerm = newName("result")
        val nullTerm = newName("isNull")
        val resultTypeTerm = boxedTypeTermForTypeInfo(resultType)
        val defaultValue = primitiveDefaultValue(resultType)

        val resultCode =
          s"""
            |$runningTerm = $running;
            |${generatedExpression.code}
            |$resultTypeTerm $resultTerm;
            |boolean $nullTerm;
            |if ($eventsName.size() > $count) {
            |  if ($first) {
            |    $resultTerm = ($resultTypeTerm) $eventsName.get($count);
            |  } else {
            |    $resultTerm = ($resultTypeTerm) $eventsName.get($eventsName.size() - $count - 1);
            |  }
            |  $nullTerm = false;
            |} else {
            |  $resultTerm = $defaultValue;
            |  $nullTerm = true;
            |}
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)

      case rexCall: RexCall =>
        val operands = rexCall.operands.asScala.map {
          operand => generateFirstLast(
            operand,
            count,
            FlinkTypeFactory.toTypeInfo(operand.getType),
            running,
            first)
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }

  private def generateRunningFinal(
      rexNode: RexNode,
      resultType: TypeInformation[_],
      running: Boolean)
    : GeneratedExpression = {
    rexNode match {
      case _: RexPatternFieldRef =>
        generateFirstLast(rexNode, 0, resultType, running, first = false)

      case rexCall: RexCall if rexCall.getOperator == FIRST || rexCall.getOperator == LAST =>
        val countLiteral = rexCall.operands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generateFirstLast(
          rexCall.operands.get(0),
          count,
          resultType,
          running,
          rexCall.getOperator == FIRST)

      case rexCall: RexCall if rexCall.getOperator.isInstanceOf[SqlAggFunction] =>
        val operandsRaw = rexCall.operands.asScala.map(generateExpression)
        val operands = operandsRaw.map {
          operand =>
            val code =
              s"""
                |$runningTerm = $running;
                |${operand.code}
                |""".stripMargin
            operand.copy(code = code)
        }
        generateCallExpression(rexCall.getOperator, operands, resultType)

      case rexCall: RexCall =>
        val operands = rexCall.operands.asScala.map {
          operand => generateRunningFinal(
            operand,
            FlinkTypeFactory.toTypeInfo(operand.getType),
            running)
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }
}
