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
package org.apache.flink.table.plan

import org.apache.calcite.sql.SqlAggFunction
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.expressions._

object MatchRecognizeTranslator {

  def extractPatternNames(pattern: Expression): Set[String] = pattern match {
    case b: BinaryPattern => extractPatternNames(b.left) ++ extractPatternNames(b.right)
    case p: PatternQuantifier => extractPatternNames(p.pattern)
    case Literal(v: String, _) => Set(v)
  }

  /**
    * Find and replace Call with RexPatternFieldRef if it is pattern field reference
    *
    * @param expr    the expression to check
    * @return replaced expression
    */
  def replacePatternFieldRef(
      patterns: Set[String],
      expr: Expression,
      table: Table)
    : Expression = expr match {
    case c @ Call(name: String, args: Seq[Expression]) if args.size == 1 &&
      args.head.isInstanceOf[UnresolvedFieldReference] =>
      val patternName = args.head.asInstanceOf[UnresolvedFieldReference].name
      if (patterns.contains(patternName)) {
        val fieldIndex = table.getSchema.columnNameToIndex(name.toLowerCase)
        val fieldType = table.getSchema.getType(fieldIndex).get
        PatternFieldRef(patternName, fieldIndex, fieldType)
      } else {
        c
      }

    // General expression
    case e: Expression =>
      val newArgs = e.productIterator.map {
        case arg: Expression => replacePatternFieldRef(patterns, arg, table)
        case seq: Traversable[_] => seq.map {
          case e: Expression => replacePatternFieldRef(patterns, e, table)
          case other => other
        }
        case other: AnyRef => other
      }
      e.makeCopy(newArgs.toArray)
  }

  /**
    * Find and replace function call such as first/last/prev/next/classifier/running/final
    *
    * @param expr    the expression to check
    * @param tableEnv the TableEnvironment
    * @return an expression with correct navigation funciton call
    */
  def replaceMatchRecognizeCall(
      expr: Expression,
      tableEnv: TableEnvironment)
    : Expression = expr match {
    // Functions calls
    case c @ Call(name, args) =>
      val function = tableEnv.getFunctionCatalog.lookupFunction(name, args)
      function match {
        case p: Prev => p
        case n: Next => n
        case l: Last => l
        case f: First => f
        case r: Running => r
        case f: Final => f
        case _ =>
          val newArgs =
            args.map(
              (exp: Expression) =>
                replaceMatchRecognizeCall(exp, tableEnv))
          c.makeCopy(Array(name, newArgs))
      }

    // General expression
    case e: Expression =>
      val newArgs = e.productIterator.map {
        case arg: Expression => replaceMatchRecognizeCall(arg, tableEnv)
        case seq: Traversable[_] => seq.map {
          case e: Expression => replaceMatchRecognizeCall(e, tableEnv)
          case other => other
        }
        case other: AnyRef => other
      }
      e.makeCopy(newArgs.toArray)
  }

  /**
    * Replace {{{A as A.price > PREV(B.price)}}} with
    * {{{PREV(A.price, 0) > LAST(B.price, 0)}}}.
    *
    * @param pattern the pattern name to define
    * @param expr    the expression to check
    * @return replaced expression
    */
  def replaceNavigation(
      pattern: String,
      expr: Expression)
    : Expression = expr match {
    case fieldRef: PatternFieldRef =>
      if (fieldRef.pattern == pattern) {
        Prev(Seq(fieldRef, Literal(0)))
      } else {
        Last(Seq(fieldRef, Literal(0)))
      }

    case e: Prev if e.children.head.isInstanceOf[PatternFieldRef] =>
      if (e.children.head.asInstanceOf[PatternFieldRef].pattern == pattern) {
        e
      } else {
        Last(e.children)
      }

    case e: First => e
    case e: Last => e
    case e: SqlAggFunction => e

    // General expression
    case e: Expression =>
      val newArgs = e.productIterator.map {
        case arg: Expression => replaceNavigation(pattern, arg)
        case seq: Traversable[_] => seq.map {
          case e: Expression => replaceNavigation(pattern, e)
          case other => other
        }
        case other: AnyRef => other
      }
      e.makeCopy(newArgs.toArray)
  }

  /**
    * Expands navigation expressions in a MATCH_RECOGNIZE clause.
    *
    * <p>Examples:
    *
    * <ul>
    * <li>{{{PREV(A.price + A.amount)}}} &rarr; {{{PREV(A.price) + PREV(A.amount)}}}
    * <li>{{{FIRST(A.price * 2)}}} &rarr; {{{FIRST(A.PRICE) * 2}}}
    * </ul>
    */
  def expandNavigation(
      expr: Expression,
      tableEnv: TableEnvironment,
      parentExpr: Option[Expression] = None,
      parentOffset: Option[Expression] = None)
    : Expression = expr match {
    case e: Expression if isPhysicalNavigation(e) || isLogicalNavigation(e) =>
      var newExpr = e.children.head
      var newOffset = e.asInstanceOf[NavigationExpression].offset

      // merge two straight prev/next, update offset
      if (isPhysicalNavigation(e)) {
        val innerExpr = e.asInstanceOf[NavigationExpression].children.head
        if (isPhysicalNavigation(innerExpr)) {
          val offset = e.asInstanceOf[NavigationExpression].offset
          val innerOffset = innerExpr.asInstanceOf[NavigationExpression].offset
          newOffset = if (e.getClass == innerExpr.getClass) {
            Plus(offset, innerOffset)
          } else {
            Minus(offset, innerOffset)
          }
          newExpr = innerExpr.makeCopy(Array(Seq(innerExpr.children.head, newOffset)))
        }
      }
      expandNavigation(newExpr, tableEnv, Some(e), Some(newOffset))

    case fieldRef: PatternFieldRef =>
      parentExpr match {
        case Some(pe) => pe.makeCopy(Array(Seq(fieldRef, parentOffset.get)))
        case None => fieldRef
      }

    // General expression
    case e: Expression =>
      val newArgs = e.productIterator.map {
        case arg: Expression =>
          parentExpr match {
            case Some(pe) =>
              pe.makeCopy(Array(Seq(expandNavigation(arg, tableEnv), parentOffset.get)))
            case None => expandNavigation(arg, tableEnv)
          }
        case seq: Traversable[_] => seq.map {
          case e: Expression =>
            parentExpr match {
              case Some(pe) =>
                pe.makeCopy(Array(Seq(expandNavigation(e, tableEnv), parentOffset.get)))
              case None => expandNavigation(e, tableEnv)
            }
          case other => other
        }
        case other: AnyRef => other
        case null => null
      }
      e.makeCopy(newArgs.toArray)
  }

  def replaceRunningOrFinalInMeasure(expr: Expression, allRows: Boolean): Expression = {
    val measure = expr.asInstanceOf[Measure]
    val oldChild = measure.child
    if (!isRunningOrFinal(oldChild) || (!allRows && oldChild.isInstanceOf[Running])) {
      val newChild = if (allRows) {
        Running(oldChild)
      } else {
        Final(oldChild)
      }
      Measure(measure.name, newChild)
    } else {
      expr
    }
  }

  private def isPhysicalNavigation(expr: Expression): Boolean = {
    expr.isInstanceOf[Next] || expr.isInstanceOf[Prev]
  }

  private def isLogicalNavigation(expr: Expression): Boolean = {
    expr.isInstanceOf[First] || expr.isInstanceOf[Last]
  }

  private def isRunningOrFinal(expr: Expression): Boolean = {
    expr.isInstanceOf[Running] || expr.isInstanceOf[Final]
  }
}
