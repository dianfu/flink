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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{FunctionLanguage, ScalarFunction}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class LogicalCalcToLogicalPythonScalarFunctionRunnerRuleTest extends TableTestBase {

  @Test
  def testOnlyOnePythonFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", PythonScalarFunction)

    val sqlQuery =
      s"""
         |SELECT
         |  pyFunc1(a, b)
         |FROM MyTable
       """.stripMargin

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonScalarFunctionRunner",
        streamTableNode(table),
        term("calls", "pyFunc1(a, b) AS f0")
      ),
      term("select", "f0 AS EXPR$0")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testChainingPythonFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", PythonScalarFunction)
    util.tableEnv.registerFunction("pyFunc2", PythonScalarFunction)

    val sqlQuery =
      s"""
         |SELECT
         |  pyFunc2(pyFunc1(a, b), c + 1)
         |FROM MyTable
       """.stripMargin

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonScalarFunctionRunner",
        streamTableNode(table),
        term("calls", "pyFunc2(pyFunc1(a, b), +(c, 1)) AS f0")
      ),
      term("select", "f0 AS EXPR$0")
    )

    util.verifySql(sqlQuery, expected)
  }
}

object PythonScalarFunction extends ScalarFunction {
  def eval(i: Int, j: Int): Int = i + j

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    BasicTypeInfo.INT_TYPE_INFO

  override def getLanguage: FunctionLanguage = FunctionLanguage.PYTHON
}
