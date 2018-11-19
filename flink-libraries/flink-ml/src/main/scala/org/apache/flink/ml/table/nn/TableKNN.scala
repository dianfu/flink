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

import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.table.common._
import org.apache.flink.ml.math.{DenseVector, Vector => FlinkVector}
import org.apache.flink.ml.metrics.distances.{DistanceMetric, EuclideanDistanceMetric, SquaredEuclideanDistanceMetric}
import org.apache.flink.ml.table.pipeline._
import org.apache.flink.ml.table.common.{OneFunction, TableFlinkMLTools, TupleIDAndData}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext, TableFunction}
import org.apache.flink.util.Collector

import org.apache.flink.api.java.tuple.{Tuple4 => JTuple4}

import scala.collection.immutable.Vector
import scala.collection.mutable
import scala.math

class TableKNN extends Predictor[TableKNN] {

  import TableKNN._

  var trainingSet: Option[Table] = None

  /** Sets K
    *
    * @param k the number of selected points as neighbors
    */
  def setK(k: Int): TableKNN = {
    require(k > 0, "K must be positive.")
    parameters.add(K, k)
    this
  }

  /** Sets the distance metric
    *
    * @param metric the distance metric to calculate distance between two points
    */
  def setDistanceMetric(metric: DistanceMetric): TableKNN = {
    parameters.add(DistanceMetric, metric)
    this
  }

  /** Sets the number of data blocks/partitions
    *
    * @param n the number of data blocks
    */
  def setBlocks(n: Int): TableKNN = {
    require(n > 0, "Number of blocks must be positive.")
    parameters.add(Blocks, n)
    this
  }

  /** Sets the Boolean variable that decides whether to use the QuadTree or not */
  def setUseQuadTree(useQuadTree: Boolean): TableKNN = {
    if (useQuadTree) {
      require(parameters(DistanceMetric).isInstanceOf[SquaredEuclideanDistanceMetric] ||
        parameters(DistanceMetric).isInstanceOf[EuclideanDistanceMetric])
    }
    parameters.add(UseQuadTree, useQuadTree)
    this
  }

  /** Parameter a user can specify if one of the training or test sets are small
    *
    * @param sizeHint cross hint tells the system which sizes to expect from the data sets
    */
  def setSizeHint(sizeHint: CrossHint): TableKNN = {
    parameters.add(SizeHint, sizeHint)
    this
  }
}

object TableKNN {

  case object K extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(5)
  }

  case object DistanceMetric extends Parameter[DistanceMetric] {
    val defaultValue: Option[DistanceMetric] = Some(EuclideanDistanceMetric())
  }

  case object Blocks extends Parameter[Int] {
    val defaultValue: Option[Int] = None
  }

  case object UseQuadTree extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = None
  }

  case object SizeHint extends Parameter[CrossHint] {
    val defaultValue: Option[CrossHint] = None
  }

  def apply(): TableKNN = {
    new TableKNN
  }

  implicit def fitKNN: FitOperation[TableKNN] = {
    new FitOperation[TableKNN] {
      override def fit(instance: TableKNN, fitParameters: ParameterMap, input: Table): Unit = {
        val resultParameters = instance.parameters ++ fitParameters

        require(resultParameters.get(K).isDefined, "K is needed for calculation")
        require(resultParameters.get(Blocks).isDefined, "Blocks is needed for calculation")

        val blocks = resultParameters.get(Blocks).get
        val partitioner = TableFlinkMLTools.ModuloKeyPartitionFunction
        val inputAsVector = input

        instance.trainingSet = Some(
          TableFlinkMLTools.block(inputAsVector, blocks, Some(partitioner)))
      }
    }
  }

  implicit def predictValues = {
    new PredictDataSetOperation[TableKNN] {
      override def predictDataSet(
          instance: TableKNN,
          predictParameters: ParameterMap,
          input: Table): Table = {
        val resultParameters = instance.parameters ++ predictParameters
        require(resultParameters.get(K).isDefined, "K is needed for calculation")
        require(resultParameters.get(Blocks).isDefined, "Blocks is needed for calculation")

        instance.trainingSet match {
          case Some(trainingSet) =>
            val k = resultParameters.get(K).get
            val blocks = resultParameters.get(Blocks).get
            val metric = resultParameters.get(DistanceMetric).get
            val partitioner = TableFlinkMLTools.ModuloKeyPartitionFunction

            val inputWithId = input.zipWithUUID()

            val dataType = input.getSchema.getFieldTypes()(0)

            val tupleIDAndData = new TupleIDAndData(dataType)

            val inputTuple = inputWithId.as('id, 'data).select(tupleIDAndData('id, 'data))

            val inputSplit = TableFlinkMLTools.block(inputTuple, blocks, Some(partitioner))

            val one = new OneFunction

            val trainTable = trainingSet.as('train).select('train, one() as 'one)
            val crossTuned = trainTable
              .join(inputSplit.as('test).select('test, one() as 'one2), 'one === 'one2)
              .select('train, 'test)

            val useQuadTree = resultParameters.get(UseQuadTree)

            val queryTableFunction = new KNNQueryTableFunction(metric, useQuadTree, k)

            val crossed = crossTuned.join(queryTableFunction('train, 'test))
              .as('train, 'test, 'singleTrain, 'singleTest, 'id, 'distance)
              .select('singleTrain, 'singleTest, 'id, 'distance)
            val sortGroupAggregateFunction = new KNNSortGroupAggregateFunction(k)

            val result = crossed.groupBy('id)
              .select(sortGroupAggregateFunction('singleTrain, 'singleTest, 'id, 'distance))
            result
        }
      }
    }
  }

  class KNNQueryTableFunction(metric: DistanceMetric, useQuadTreeOption: Option[Boolean], k: Int)
    extends TableFunction[JTuple4[FlinkVector, FlinkVector, String, Double]] {
    var out: Collector[JTuple4[FlinkVector, FlinkVector, String, Double]] = _

    override def open(context: FunctionContext): Unit = {
      super.open(context)
      out = new Collector[JTuple4[FlinkVector, FlinkVector, String, Double]] {
        /**
          * Emits a record.
          *
          * @param record The record to collect.
          */
        override def collect(record: JTuple4[FlinkVector, FlinkVector, String, Double]): Unit = {
          KNNQueryTableFunction.this.collect(record)
        }

        /**
          * Closes the collector. If any data was buffered, that data will be flushed.
          */
        override def close(): Unit = {}
      }
    }

    def eval(training: Block[FlinkVector], testing: Block[JTuple2[String, FlinkVector]]): Unit = {
      // use a quadtree if (4 ^ dim) * Ntest * log(Ntrain)
      // < Ntest * Ntrain, and distance is Euclidean
      val checkSize = math.log(4.0) * training.values.head.size +
        math.log(math.log(training.values.length)) < math.log(training.values.length)
      val checkMetric = metric match {
        case _: EuclideanDistanceMetric => true
        case _: SquaredEuclideanDistanceMetric => true
        case _ => false
      }
      val useQuadTree = useQuadTreeOption.getOrElse(checkSize && checkMetric)

      if (useQuadTree) {
        knnQueryWithQuadTree(training.values, testing.values, k, metric, out)
      } else {
        knnQueryBasic(training.values, testing.values, k, metric, out)
      }
    }
  }

  class KNNSortGroupAggregateFunction(k: Int)
    extends AggregateFunction[(FlinkVector, Array[FlinkVector]),
      mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]]] {
    /**
      * Creates and init the Accumulator for this [[AggregateFunction]].
      *
      * @return the accumulator with the initial value
      */
    override def createAccumulator()
        : mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]] = {
      mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]]()(
        Ordering.by(_.f3))
    }

    def accumulate(
        accumulator: mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]],
        train: FlinkVector, test: FlinkVector, id: String, distance: Double): Unit = {
      accumulator.enqueue(JTuple4.of(train, test, id, distance))
      if (accumulator.size > k) {
        accumulator.dequeue()
      }
    }

    /**
      * Called every time when an aggregation result should be materialized.
      * The returned value could be either an early and incomplete result
      * (periodically emitted as data arrive) or the final result of the
      * aggregation.
      *
      * @param accumulator the accumulator which contains the current
      *                    aggregated results
      * @return the aggregation result
      */
    override def getValue(
        accumulator: mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]])
        : (FlinkVector, Array[FlinkVector]) = {
      if (accumulator.nonEmpty) {
        (accumulator.head.f1, accumulator.map(_.f0).toArray)
      } else {
        (null.asInstanceOf[FlinkVector], Array())
      }
    }

    def resetAccumulator(
        accumulator: mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]])
        : Unit = {
      accumulator.clear()
    }
  }

  private def knnQueryWithQuadTree[T <: FlinkVector](
      training: Vector[T],
      testing: Vector[JTuple2[String, T]],
      k: Int,
      metric: DistanceMetric,
      out: Collector[JTuple4[FlinkVector, FlinkVector, String, Double]]): Unit = {
    // find a bounding box
    val MinArr = Array.tabulate(training.head.size)(x => x)
    val MaxArr = Array.tabulate(training.head.size)(x => x)

    val minVecTrain = MinArr.map(i => training.map(x => x(i)).min - 0.01)
    val minVecTest = MinArr.map(i => testing.map(x => x.f1(i)).min - 0.01)
    val maxVecTrain = MaxArr.map(i => training.map(x => x(i)).max + 0.01)
    val maxVecTest = MaxArr.map(i => testing.map(x => x.f1(i)).max + 0.01)

    val MinVec = DenseVector(MinArr.map(i => math.min(minVecTrain(i), minVecTest(i))))
    val MaxVec = DenseVector(MinArr.map(i => math.max(maxVecTrain(i), maxVecTest(i))))

    // default value of max elements/box is set to max(20,k)
    val maxPerBox = math.max(k, 20)
    val trainingQuadTree = new QuadTree(MinVec, MaxVec, metric, maxPerBox)

    val queue = mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]]()(
      Ordering.by(_.f3))

    for (v <- training) {
      trainingQuadTree.insert(v)
    }

    for (idWithVector <- testing) {
      val id = idWithVector.f0
      val vector = idWithVector.f1
      // Find siblings' objects and do local kNN there
      val siblingObjects = trainingQuadTree.searchNeighborsSiblingQueue(vector)

      // do KNN query on siblingObjects and get max distance of kNN then rad is good choice
      // for a neighborhood to do a refined local kNN search
      val knnSiblings = siblingObjects.map(v => metric.distance(vector, v)).sortWith(_ < _).take(k)

      val rad = knnSiblings.last
      val trainingFiltered = trainingQuadTree.searchNeighbors(vector, rad)

      for (b <- trainingFiltered) {
        // (training vector, input vector, input key, distance)
        queue.enqueue(JTuple4.of(b, vector, id, metric.distance(b, vector)))
        if (queue.size > k) {
          queue.dequeue()
        }
      }

      for (v <- queue) {
        out.collect(v)
      }
    }
  }

  private def knnQueryBasic[T <: FlinkVector](
      training: Vector[T],
      testing: Vector[JTuple2[String, T]],
      k: Int,
      metric: DistanceMetric,
      out: Collector[JTuple4[FlinkVector, FlinkVector, String, Double]]): Unit = {
    val queue = mutable.PriorityQueue[JTuple4[FlinkVector, FlinkVector, String, Double]]()(
      Ordering.by(_.f3))

    for (idWithVector <- testing) {
      for (b <- training) {
        // (training vector, input vector, input key, distance)
        queue.enqueue(JTuple4.of(b, idWithVector.f1, idWithVector.f0, metric.distance(b, idWithVector.f1)))
        if (queue.size > k) {
          queue.dequeue()
        }
      }

      for (v <- queue) {
        out.collect(v)
      }
    }
  }
}
