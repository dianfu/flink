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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class CepITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testSimpleCEP(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val table = env.fromCollection(data).toTable(tEnv, 'id, 'name)

    val matchTable = table
      .matchRecognize()
      .pattern("(A B C)")
      .define("A AS A.name = 'a'")
      .define("B AS B.name = 'b', C AS C.name = 'c'")
      .measures("A.id AS aid, B.id AS bid")
      .measures("C.id AS cid")
      .build()

    val result = matchTable.toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("6,7,8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPartitionByOrderByEventTime() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3L, ("ACME", 17, 2))))
    data.+=(Left((1L, ("ACME", 12, 1))))
    data.+=(Left((2L, ("BCME", 12, 1))))
    data.+=(Left((4L, ("BCME", 17, 2))))
    data.+=(Right(4L))
    data.+=(Left((5L, ("ACME", 13, 3))))
    data.+=(Left((7L, ("ACME", 15, 4))))
    data.+=(Left((8L, ("BCME", 15, 4))))
    data.+=(Left((6L, ("BCME", 13, 3))))
    data.+=(Right(8L))
    data.+=(Left((10L, ("BCME", 20, 5))))
    data.+=(Left((9L, ("ACME", 20, 5))))
    data.+=(Right(13L))
    data.+=(Left((15L, ("ACME", 19, 8))))
    data.+=(Left((16L, ("BCME", 19, 8))))
    data.+=(Right(16L))

    val table = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)

    val matchTable = table
      .matchRecognize()
      .partitionBy("symbol")
      .orderBy("tstamp")
      .measures(
        s"""
          |STRT.tstamp AS start_tstamp,
          |FIRST(DOWN.tstamp) AS bottom_tstamp,
          |FIRST(UP.tstamp) AS end_tstamp,
          |FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
          |FIRST(UP.price + UP.tax) AS end_total
         """.stripMargin)
      .pattern("(STRT DOWN+ UP+)")
      .allRows(false)
      .define(
        s"""
          |DOWN AS DOWN.price < PREV(DOWN.price),
          |UP AS UP.price > PREV(UP.price)
         """.stripMargin)
      .build()

    val result = matchTable.toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:00.003,1970-01-01 00:00:00.005,1970-01-01 00:00:00.007,17,19",
      "ACME,1970-01-01 00:00:00.003,1970-01-01 00:00:00.005,1970-01-01 00:00:00.007,17,19",
      "BCME,1970-01-01 00:00:00.004,1970-01-01 00:00:00.006,1970-01-01 00:00:00.008,17,19",
      "BCME,1970-01-01 00:00:00.004,1970-01-01 00:00:00.006,1970-01-01 00:00:00.008,17,19")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
