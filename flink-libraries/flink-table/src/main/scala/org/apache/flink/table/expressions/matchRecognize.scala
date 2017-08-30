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
package org.apache.flink.table.expressions

import org.apache.calcite.rex.{RexNode, RexPatternFieldRef}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlMatchRecognize, SqlOperator}
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{FINAL, FIRST, LAST, NEXT, PATTERN_ALTER, PATTERN_CONCAT, PATTERN_QUANTIFIER, PREV, RUNNING}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

import scala.collection.JavaConversions._

case class Quantifier(startNum: Int, endNum: Int, isReluctant: Boolean) extends Expression {
  override def resultType: TypeInformation[_] = null

  override def toString: String = if (startNum == endNum) {
    "{ " + startNum + " }"
  } else {
    val str = if (startNum == 0 && endNum == -1) {
      "*"
    } else if (startNum == 1 && endNum == -1) {
      "+"
    } else if (endNum == -1) {
      "{ " + startNum + ", }"
    } else if (startNum == 0 && endNum == 1) {
      "?"
    } else if (startNum == -1) {
      "{ , " + endNum + " }"
    } else {
      "{ " + startNum + ", " + endNum + " }"
    }

    if (isReluctant) {
      str + "?"
    } else {
      str
    }
  }

  override private[flink] def validateInput(): ValidationResult = {
    if ((endNum == -1 && startNum < 0) || (endNum != -1 && endNum < startNum)) {
      ValidationFailure(s"Invalid Quantifier: $toString")
    } else {
      ValidationSuccess
    }
  }

  override private[flink] def children =
    Seq(Literal(startNum), Literal(endNum), Literal(isReluctant))
}

abstract class BinaryPattern extends BinaryExpression {
  private[flink] def sqlOperator: SqlOperator

  override private[flink] def resultType = null

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.getRexBuilder.makeCall(
      relBuilder.getTypeFactory.createSqlType(SqlTypeName.NULL),
      sqlOperator,
      children.map(_.toRexNode))
  }
}

case class PatternConcat(left: Expression, right: Expression) extends BinaryPattern {
  override def toString = s"$left $right"

  override private[flink] def sqlOperator = PATTERN_CONCAT
}

case class PatternAlter(left: Expression, right: Expression) extends BinaryPattern {
  override def toString = s"$left | $right"

  override private[flink] def sqlOperator = PATTERN_ALTER
}

case class PatternQuantifier(pattern: Expression, quantifier: Expression) extends Expression {
  override def toString = s"$pattern$quantifier"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.getRexBuilder.makeCall(
      relBuilder.getTypeFactory.createSqlType(SqlTypeName.NULL),
      PATTERN_QUANTIFIER,
      children.map(_.toRexNode))
  }

  override private[flink] def resultType = null

  override private[flink] def children: Seq[Expression] = {
    Seq(pattern) ++ quantifier.asInstanceOf[Quantifier].children
  }
}

case class PatternFieldRef(pattern: String, fieldIndex: Int, fieldType: TypeInformation[_])
    extends LeafExpression {
  override private[flink] def resultType = fieldType

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val fieldRelType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createTypeFromTypeInfo(resultType, isNullable = true)
    new RexPatternFieldRef(pattern, fieldIndex, fieldRelType)
  }
}

case class PatternDefination(name: String, child: Expression) extends UnaryExpression {
  override def toString = s"$name as $child"

  override private[flink] def resultType = null

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    child.toRexNode
  }
}

case class Measure(name: String, child: Expression) extends UnaryExpression {
  override def toString = s"$child as $name"

  override private[flink] def resultType = child.resultType

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.alias(child.toRexNode, name)
  }
}

case class AfterSymbol(afterOption: Int, sym: String = null)

case class AfterMatch(after: AfterSymbol) extends LeafExpression {
  override def toString: String = if (after.afterOption == 1) {
    s"after match skip to next row"
  } else if (after.afterOption == 2) {
    s"after match skip past last row"
  } else if (after.afterOption == 3) {
    s"after match skip to first ${after.sym}"
  } else {
    s"after match skip to last ${after.sym}"
  }

  override private[flink] def resultType = null

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    if (after.afterOption == 1) {
      relBuilder.getRexBuilder.makeFlag(SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW)
    } else if (after.afterOption == 2) {
      relBuilder.getRexBuilder.makeFlag(SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW)
    } else if (after.afterOption == 3) {
      relBuilder.getRexBuilder.makeCall(
        relBuilder.getTypeFactory.createSqlType(SqlTypeName.NULL),
        SqlMatchRecognize.SKIP_TO_FIRST,
        Lists.newArrayList(relBuilder.literal(after.sym)))
    } else {
      relBuilder.getRexBuilder.makeCall(
        relBuilder.getTypeFactory.createSqlType(SqlTypeName.NULL),
        SqlMatchRecognize.SKIP_TO_LAST,
        Lists.newArrayList(relBuilder.literal(after.sym)))
    }
  }
}

abstract class NavigationExpression(args: Seq[Expression]) extends Expression {
  private[flink] def sqlOperator: SqlOperator

  private[flink] def physical: Boolean

  val offset: Expression = if (args.length == 1) {
    if (physical) {
      Literal(1)
    } else {
      Literal(0)
    }
  } else {
    args(1)
  }

  override private[flink] def resultType = args.head.resultType

  override private[flink] def children =
    Seq(args.head, offset)

  override private[flink] def validateInput(): ValidationResult = {
    if (args.isEmpty || args.length > 2) {
      ValidationFailure(s"$this can only have one or two operands.")
    } else if (args.length == 2 && !TypeCheckUtils.isNumeric(args(1).resultType)) {
      ValidationFailure(s"The second operand of $this requires Numeric, get " +
                          s"${args(1).resultType}")
    } else {
      ValidationSuccess
    }
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(sqlOperator, children.map(_.toRexNode))
  }
}

case class First(args: Seq[Expression]) extends NavigationExpression(args) {
  override private[flink] def sqlOperator = FIRST

  override private[flink] def physical = false
}

case class Last(args: Seq[Expression]) extends NavigationExpression(args) {
  override private[flink] def sqlOperator = LAST

  override private[flink] def physical = false
}

case class Prev(args: Seq[Expression]) extends NavigationExpression(args) {
  override private[flink] def sqlOperator = PREV

  override private[flink] def physical = true
}

case class Next(args: Seq[Expression]) extends NavigationExpression(args) {
  override private[flink] def sqlOperator = NEXT

  override private[flink] def physical = true
}

case class Running(child: Expression) extends UnaryExpression {
  override private[flink] def resultType = child.resultType

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    relBuilder.call(RUNNING, child.toRexNode)
}

case class Final(child: Expression) extends UnaryExpression {
  override private[flink] def resultType = child.resultType

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    relBuilder.call(FINAL, child.toRexNode)
}
