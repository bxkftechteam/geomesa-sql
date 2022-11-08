/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spatialx.geomesa.sql.enumerator
import org.apache.calcite.linq4j.function.Predicate2
import org.apache.calcite.linq4j.{Enumerable, Enumerator, function}
import com.spatialx.geomesa.sql.GeomLambdaExpression
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.strtree.STRtree

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer

/**
 * Produces results of spatial join using spatial index
 */
class EnumerableSpatialJoinEnumerator(
  cancelFlag: AtomicBoolean,
  left: Enumerable[AnyRef],
  right: Enumerable[AnyRef],
  resultSelector: function.Function2[AnyRef, AnyRef, AnyRef],
  leftGeomEvaluator: GeomLambdaExpression.GeomFunction1[AnyRef],
  rightGeomEvaluator: GeomLambdaExpression.GeomFunction1[AnyRef],
  within: Double,
  generateNullsOnRight: Boolean,
  predicate: Predicate2[AnyRef, AnyRef]) extends Enumerator[AnyRef] {

  private val leftEnumerator = left.enumerator()
  private val rightEnumerator = right.enumerator()
  private val spatialIndex = new STRtree()
  private var builtIndex = false
  private val currentResults = ArrayBuffer.empty[AnyRef]
  private var currentResultIndex = 0
  private var currentResult: AnyRef = _

  override def current(): AnyRef = currentResult

  override def moveNext(): Boolean = {
    if (cancelFlag.get()) false else {
      doMoveNext()
    }
  }

  private def doMoveNext(): Boolean = {
    if (currentResultIndex < currentResults.length) {
      currentResult = currentResults(currentResultIndex)
      currentResultIndex += 1
      true
    } else {
      if (!builtIndex) {
        indexRightSide()
        builtIndex = true
      }
      loadFromLeftSide()
      currentResultIndex = 0
      if (currentResults.isEmpty) false else {
        currentResult = currentResults(0)
        currentResultIndex = 1
        true
      }
    }
  }

  private def loadFromLeftSide(): Unit = {
    currentResults.clear()
    while (!cancelFlag.get() && currentResults.isEmpty && leftEnumerator.moveNext()) {
      val leftRow = leftEnumerator.current()
      val geom = leftGeomEvaluator.apply(leftRow)
      if (geom != null) {
        val env = getEnvelope(geom, within)
        val results = spatialIndex.query(env)
        results.forEach { rightRow =>
          if (predicate.apply(leftRow, rightRow.asInstanceOf[AnyRef])) {
            currentResults += resultSelector.apply(leftRow, rightRow.asInstanceOf[AnyRef])
          }
        }
        if (currentResults.isEmpty && generateNullsOnRight) {
          currentResults += resultSelector.apply(leftRow, null)
        }
      }
    }
  }

  private def indexRightSide(): Unit = {
    while (!cancelFlag.get() && rightEnumerator.moveNext()) {
      val rightRow = rightEnumerator.current()
      val geom = rightGeomEvaluator.apply(rightRow)
      if (geom != null) {
        val env = getEnvelope(geom, 0)
        spatialIndex.insert(env, rightRow)
      }
    }
  }

  private def getEnvelope(geom: Geometry, within: Double): Envelope = {
    val envBound = geom.getEnvelopeInternal
    new Envelope(envBound.getMinX - within, envBound.getMaxX + within, envBound.getMinY - within, envBound.getMaxY + within)
  }

  override def reset(): Unit = {
    leftEnumerator.reset()
    rightEnumerator.reset()
    currentResults.clear()
    currentResultIndex = 0
    currentResult = null
  }

  override def close(): Unit = {
    leftEnumerator.close()
    rightEnumerator.close()
  }
}
