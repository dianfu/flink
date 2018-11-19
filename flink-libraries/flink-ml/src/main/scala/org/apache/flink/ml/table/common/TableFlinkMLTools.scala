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

package org.apache.flink.ml.table.common

import java.util.Random

import com.fasterxml.uuid.Generators
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext, ScalarFunction}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo

import scala.collection.mutable.ArrayBuffer

object TableFlinkMLTools {
  def block(input: Table, numBlocks: Int, partitionerOption: Option[ScalarFunction]): Table = {
    val datatype = input.getSchema.getFieldTypes()(0)
    val blockIDAssigner = new BlockIDAssigner(datatype, numBlocks)
    val blockIDInput = input
      .as('data)
      .select(blockIDAssigner('data).flatten())
      .as('id, 'data)

    val preGroupBlockIDInput = partitionerOption match {
      case Some(partitioner) =>
        blockIDInput.select(partitioner('id, numBlocks) as 'id, 'data)

      case None => blockIDInput
    }

    val blockGenerator = new BlockGenerator
    preGroupBlockIDInput.groupBy('id).select(blockGenerator('id, 'data))
  }

  object ModuloKeyPartitionFunction extends ScalarFunction {
    def eval(key: Int, numPartitions: Int): Int = {
      val result = key % numPartitions

      if(result < 0) {
        result + numPartitions
      } else {
        result
      }
    }

    def eval(key: String, numPartitions: Int): Int = {
      val result = key.hashCode % numPartitions

      if (result < 0) {
        result + numPartitions
      } else {
        result
      }
    }
  }
}

class BlockIDAssigner(dataType: TypeInformation[_], numBlocks: Int) extends ScalarFunction {
  def eval(element: Any): JTuple2[Int, Any] = {
    val blockID = element.hashCode() % numBlocks

    val blockIDResult = if(blockID < 0){
      blockID + numBlocks
    } else {
      blockID
    }

    JTuple2.of(blockIDResult, element)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    new TupleTypeInfo(Types.INT, dataType)
  }
}

class BlockGenerator extends AggregateFunction[Block[Any], JTuple2[Integer, ArrayBuffer[Any]]] {
  /**
    * Creates and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  override def createAccumulator(): JTuple2[Integer, ArrayBuffer[Any]] =
    JTuple2.of(0, ArrayBuffer[Any]())

  def resetAccumulator(accumulator: JTuple2[Integer, ArrayBuffer[Any]]): Unit = {
    accumulator.f0 = 0
    accumulator.f1.clear()
  }

  def accumulate(acc: JTuple2[Integer, ArrayBuffer[Any]], id: Integer, element: Any): Unit = {
    acc.f0 = id
    acc.f1.append(element)
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
  override def getValue(accumulator: JTuple2[Integer, ArrayBuffer[Any]]): Block[Any] = {
    Block(accumulator.f0, accumulator.f1.toVector)
  }
}

class BroadcastSingleElementMapperFunction[T, B, O](fun: (T, B) => O)
  extends ScalarFunction {
  def eval(value: T, broadcast: B): O = {
    fun(value, broadcast)
  }
}

class BroadcastSingleElementFilterFunction[T, B](fun: (T, B) => Boolean)
  extends ScalarFunction {
  def eval(value: T, broadcast: B): Boolean = {
    fun(value, broadcast)
  }
}

class RandomLongFunction extends ScalarFunction {

  var r: Random = _

  override def open(context: FunctionContext): Unit = {
    super.open(context)
    r = new Random
  }

  override def isDeterministic: Boolean = false

  def eval(): Long = r.nextLong()
}

class UUIDGenerateFunction extends ScalarFunction {
  def eval(): String = Generators.timeBasedGenerator().generate().toString

  override def isDeterministic: Boolean = false
}

class TupleIDAndData(val dataType: TypeInformation[_]) extends ScalarFunction {

  def eval(id: String, data: Any): JTuple2[String, Any] = {
    JTuple2.of(id, data)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    new TupleTypeInfo(Types.STRING, dataType)
}

class OneFunction extends ScalarFunction {
  def eval(): Int = {
    1
  }

  override def isDeterministic: Boolean = false
}
