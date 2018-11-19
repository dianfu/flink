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

package org.apache.flink.ml.table.pipeline

import org.apache.flink.ml.table.common.{ParameterMap, WithParameters}
import org.apache.flink.table.api.Table

import scala.reflect.runtime.universe._

/** Base trait for Flink's pipeline operators.
  *
  * An estimator can be fitted to input data. In order to do that the implementing class has
  * to provide an implementation of a [[FitOperation]] with the correct input type. In order to make
  * the [[FitOperation]] retrievable by the Scala compiler, the implementation should be placed
  * in the companion object of the implementing class.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @tparam Self
  */
trait Estimator[Self] extends WithParameters {
  that: Self =>

  /** Fits the estimator to the given input data. The fitting logic is contained in the
    * [[FitOperation]]. The computed state will be stored in the implementing class.
    *
    * @param training Training data
    * @param fitParameters Additional parameters for the [[FitOperation]]
    * @param fitOperation [[FitOperation]] which encapsulates the algorithm logic
    * @return
    */
  def fit(
      training: Table,
      fitParameters: ParameterMap = ParameterMap.Empty)(implicit
      fitOperation: FitOperation[Self]): Unit = {
    fitOperation.fit(this, fitParameters, training)
  }
}

object Estimator{

  /** Fallback [[FitOperation]] type class implementation which is used if no other
    * [[FitOperation]] with the right input types could be found in the scope of the implementing
    * class. The fallback [[FitOperation]] makes the system fail in the pre-flight phase by
    * throwing a [[RuntimeException]] which states the reason for the failure. Usually the error
    * is a missing [[FitOperation]] implementation for the input types or the wrong chaining
    * of pipeline operators which have incompatible input/output types.
    *
     * @tparam Self Type of the pipeline operator
    * @return
    */
  implicit def fallbackFitOperation[
      Self: TypeTag]
    : FitOperation[Self] = {
    new FitOperation[Self]{
      override def fit(
          instance: Self,
          fitParameters: ParameterMap,
          input: Table)
        : Unit = {
        val self = typeOf[Self]

        throw new RuntimeException("There is no FitOperation defined for " + self +
          " which trains on a DataSet[" +  "]")
      }
    }
  }

  /** Fallback [[PredictDataSetOperation]] if a [[Predictor]] is called with a not supported input
    * data type. The fallback [[PredictDataSetOperation]] lets the system fail with a
    * [[RuntimeException]] stating which input and output data types were inferred but for which no
    * [[PredictDataSetOperation]] could be found.
    *
    * @tparam Self Type of the [[Predictor]]
    * @return
    */
  implicit def fallbackPredictOperation[
      Self: TypeTag]
    : PredictDataSetOperation[Self] = {
    new PredictDataSetOperation[Self] {
      override def predictDataSet(
          instance: Self,
          predictParameters: ParameterMap,
          input: Table): Table = {
        val self = typeOf[Self]

        throw new RuntimeException("There is no PredictOperation defined for " + self +
          " which takes a DataSet[" + "" + "] as input.")
      }
    }
  }

  /** Fallback [[TransformDataSetOperation]] for [[Transformer]] which do not support the input or
    * output type with which they are called. This is usually the case if pipeline operators are
    * chained which have incompatible input/output types. In order to detect these failures, the
    * fallback [[TransformDataSetOperation]] throws a [[RuntimeException]] with the corresponding
    * input/output types. Consequently, a wrong pipeline will be detected at pre-flight phase of
    * Flink and thus prior to execution time.
    *
    * @tparam Self Type of the [[Transformer]] for which the [[TransformDataSetOperation]] is
    *              defined
    * @return
    */
  implicit def fallbackTransformOperation[
  Self: TypeTag]
  : TransformDataSetOperation[Self] = {
    new TransformDataSetOperation[Self] {
      override def transformDataSet(
        instance: Self,
        transformParameters: ParameterMap,
        input: Table)
      : Table = {
        val self = typeOf[Self]

        throw new RuntimeException("There is no TransformOperation defined for " +
          self +  " which takes a DataSet[" + "" +
          "] as input.")
      }
    }
  }

  implicit def fallbackEvaluateOperation[
      Self: TypeTag]
    : EvaluateDataSetOperation[Self] = {
    new EvaluateDataSetOperation[Self] {
      override def evaluateDataSet(
        instance: Self,
        predictParameters: ParameterMap,
        input: Table)
      : Table = {
        val self = typeOf[Self]

        throw new RuntimeException("There is no PredictOperation defined for " + self +
          " which takes a DataSet[" + "" + "] as input.")
      }
    }
  }
}

/** Type class for the fit operation of an [[Estimator]].
  *
  * The [[FitOperation]] contains a self type parameter so that the Scala compiler looks into
  * the companion object of this class to find implicit values.
  *
  * @tparam Self Type of the [[Estimator]] subclass for which the [[FitOperation]] is defined
  */
trait FitOperation[Self]{
  def fit(instance: Self, fitParameters: ParameterMap,  input: Table): Unit
}
