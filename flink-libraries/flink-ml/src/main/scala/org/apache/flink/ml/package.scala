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

package org.apache.flink

import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.table.common.{BroadcastSingleElementFilterFunction, BroadcastSingleElementMapperFunction, UUIDGenerateFunction}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table

import scala.reflect.ClassTag

package object ml {

  /** Pimp my [[ExecutionEnvironment]] to directly support `readLibSVM`
    *
    * @param executionEnvironment
    */
  implicit class RichExecutionEnvironment(executionEnvironment: ExecutionEnvironment) {
    def readLibSVM(path: String): DataSet[LabeledVector] = {
      MLUtils.readLibSVM(executionEnvironment, path)
    }
  }

  /** Pimp my [[DataSet]] to directly support `writeAsLibSVM`
    *
    * @param dataSet
    */
  implicit class RichLabeledDataSet(dataSet: DataSet[LabeledVector]) {
    def writeAsLibSVM(path: String): DataSink[String] = {
      MLUtils.writeLibSVM(path, dataSet)
    }
  }

  implicit class RichDataSet[T](dataSet: DataSet[T]) {
    def mapWithBcVariable[B, O: TypeInformation: ClassTag](
        broadcastVariable: DataSet[B])(
        fun: (T, B) => O)
      : DataSet[O] = {
      dataSet.map(new BroadcastSingleElementMapper[T, B, O](dataSet.clean(fun)))
        .withBroadcastSet(broadcastVariable, "broadcastVariable")
    }

    def filterWithBcVariable[B, O](broadcastVariable: DataSet[B])(fun: (T, B) => Boolean)
      : DataSet[T] = {
      dataSet.filter(new BroadcastSingleElementFilter[T, B](dataSet.clean(fun)))
        .withBroadcastSet(broadcastVariable, "broadcastVariable")
    }

    def mapWithBcVariableIteration[B, O: TypeInformation: ClassTag](
        broadcastVariable: DataSet[B])(fun: (T, B, Int) => O)
      : DataSet[O] = {
      dataSet.map(new BroadcastSingleElementMapperWithIteration[T, B, O](dataSet.clean(fun)))
        .withBroadcastSet(broadcastVariable, "broadcastVariable")
    }
  }

  private class BroadcastSingleElementMapper[T, B, O](
      fun: (T, B) => O)
    extends RichMapFunction[T, O] {
    var broadcastVariable: B = _

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      broadcastVariable = getRuntimeContext.getBroadcastVariable[B]("broadcastVariable").get(0)
    }

    override def map(value: T): O = {
      fun(value, broadcastVariable)
    }
  }

  private class BroadcastSingleElementMapperWithIteration[T, B, O](
      fun: (T, B, Int) => O)
    extends RichMapFunction[T, O] {
    var broadcastVariable: B = _

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      broadcastVariable = getRuntimeContext.getBroadcastVariable[B]("broadcastVariable").get(0)
    }

    override def map(value: T): O = {
      fun(value, broadcastVariable, getIterationRuntimeContext.getSuperstepNumber)
    }
  }

  private class BroadcastSingleElementFilter[T, B](
      fun: (T, B) => Boolean)
    extends RichFilterFunction[T] {
    var broadcastVariable: B = _

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      broadcastVariable = getRuntimeContext.getBroadcastVariable[B]("broadcastVariable").get(0)
    }

    override def filter(value: T): Boolean = {
      fun(value, broadcastVariable)
    }
  }

  implicit class RichTable(table: Table) {
    def mapWithBcVariable[T, B, O: TypeInformation: ClassTag](broadcastVariable: Table)
                                                             (fun: (T, B) => O)
    : Table = {
      val func = new BroadcastSingleElementMapperFunction(fun)
      table.as('value).join(broadcastVariable.as('broadcast))
        .select(func('value, 'broadcast))
    }

    def filterWithBcVariable[T, B, O](broadcastVariable: Table)(fun: (T, B) => Boolean)
    : Table = {
      val func = new BroadcastSingleElementFilterFunction(fun)
      table.as('value).join(broadcastVariable.as('broadcast))
        .where(func('value, 'broadcast))
    }

    def zipWithUUID[T](): Table = {
      val func = new UUIDGenerateFunction
      table.as('t).select(func() as 'id, 't)
    }

    def mapPartition(func: Any): Table = { //fake
      table
    }
  }
}
