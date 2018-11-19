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

package org.apache.flink.ml.table.nn

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.classification.Classification
import org.apache.flink.ml.math.{DenseVector, Vector => FlinkVector}
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.junit.Test

class TableKNNITSuite {
  // calculate answer
  val answer: Array[DenseVector] = Classification.trainingData.map {
    v => (v.vector.asInstanceOf[DenseVector], SquaredEuclideanDistanceMetric().distance(DenseVector(0.0, 0.0), v.vector))
  }.sortBy(_._2).take(3).map(_._1).toArray

  @Test
  def testTableKNN(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // prepare data
    val trainingSet = env.fromCollection(Classification.trainingData)
      .map({e => (e.vector.asInstanceOf[DenseVector], 0)})
    val testingSet = env.fromElements((DenseVector(0.0, 0.0), 0))

    val tEnv = TableEnvironment.getTableEnvironment(env)

    val trainTable = trainingSet.toTable(tEnv, 'data, 'zero).select('data)
    val testTable = testingSet.toTable(tEnv, 'data, 'zero).select('data)

    val knn = TableKNN()
      .setK(3)
      .setBlocks(10)
      .setDistanceMetric(SquaredEuclideanDistanceMetric())
      .setUseQuadTree(false)

    knn.fit(trainTable)
    val result: Seq[Row] = knn.predict(testTable).toDataSet[Row].collect()
    val row: Row = result.head
    val tup: (DenseVector, Array[FlinkVector]) =
      row.getField(0).asInstanceOf[(DenseVector, Array[FlinkVector])]
    val arr: Array[FlinkVector] = tup._2
    val sortedArr = arr.map(_.toString).sortWith((a, b) => a > b)
    val sortedAnswer = answer.map(_.toString).sortWith((a, b) => a > b)
    assert(
      sortedArr
      .sameElements(
        sortedAnswer))
    println("actual computed knn result: ")
    sortedArr.foreach(println(_))
    println("expected result: ")
    sortedAnswer.foreach(println(_))
  }
}
