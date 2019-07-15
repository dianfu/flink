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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class PythonScalarFunctionSplitRuleTest extends TableTestBase {

  @Test
  def testPythonFunctionAsInputOfJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", PythonScalarFunction)

    val sqlQuery =
      s"""
         |SELECT
         |  pyFunc1(a, b) + 1
         |FROM MyTable
       """.stripMargin

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamPythonScalarFunctionRunner",
          streamTableNode(table),
          term("calls", "pyFunc1(a, b) AS f0")
        ),
        term("select", "a, b, c, f0")
      ),
      term("select", "+(f0, 1) AS EXPR$0")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testPythonFunctionMixWithJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", PythonScalarFunction)

    val sqlQuery =
      s"""
         |SELECT
         |  pyFunc1(a, b), c + 1
         |FROM MyTable
       """.stripMargin

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonScalarFunctionRunner",
        streamTableNode(table),
        term("calls", "pyFunc1(a, b) AS f0")
      ),
      term("select", "f0 AS EXPR$0", "+(c, 1) AS EXPR$1")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testChainingPythonFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", PythonScalarFunction)
    util.tableEnv.registerFunction("pyFunc2", PythonScalarFunction)
    util.tableEnv.registerFunction("pyFunc3", PythonScalarFunction)

    val sqlQuery =
      s"""
         |SELECT
         |  pyFunc3(pyFunc2(a + pyFunc1(a, c), b), c)
         |FROM MyTable
       """.stripMargin

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonScalarFunctionRunner",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamPythonScalarFunctionRunner",
            streamTableNode(table),
            term("calls", "pyFunc1(a, c) AS f0")
          ),
          term("select", "a, b, c, f0")
        ),
        term("calls", "pyFunc3(pyFunc2(+(a, f0), b), c) AS f04")
      ),
      term("select", "f04 AS EXPR$0")
    )

    util.verifySql(sqlQuery, expected)
  }
}
