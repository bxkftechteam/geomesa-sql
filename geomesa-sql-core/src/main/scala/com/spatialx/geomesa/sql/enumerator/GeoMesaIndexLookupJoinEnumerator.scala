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

import org.apache.calcite.linq4j.function.{EqualityComparer, Function1, Predicate2}
import org.apache.calcite.linq4j.{Enumerable, Enumerator}
import org.geotools.factory.CommonFactoryFinder
import com.spatialx.geomesa.sql.GeoMesaTranslatableTable.GeoMesaQueryable
import com.spatialx.geomesa.sql.enumerator.AttributeConverter.convertLocalTimestamp
import com.spatialx.geomesa.sql.enumerator.GeoMesaIndexLookupJoinEnumerator.{LOOKUP_BATCH_SIZE, SCAN_BATCH_SIZE, ff}
import com.spatialx.geomesa.sql.{FID_FIELD_NAME, GeoMesaQueryParams, GeoMesaTranslatableTable}
import org.locationtech.geomesa.filter.{andFilters, orFilters}
import org.opengis.filter.{Filter, FilterFactory2}

import java.sql.Timestamp
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Produces joined result by performing index lookup join
 */
class GeoMesaIndexLookupJoinEnumerator(
  cancelFlag: AtomicBoolean,
  left: Enumerable[AnyRef],
  leftKeysSelector: Function1[AnyRef, AnyRef],
  right: GeoMesaQueryParams,
  rightQueryable: GeoMesaQueryable[AnyRef],
  rightKeys: Array[Int],
  rightKeysSelector: Function1[AnyRef, AnyRef],
  resultSelector: org.apache.calcite.linq4j.function.Function2[AnyRef, AnyRef, AnyRef],
  comparer: EqualityComparer[AnyRef],
  generateNullsOnLeft: Boolean,
  generateNullsOnRight: Boolean,
  predicate: Predicate2[AnyRef, AnyRef]) extends Enumerator[AnyRef] {

  import scala.collection.JavaConverters._

  private val leftEnumerator = left.enumerator()
  private val rightTable = rightQueryable.table.asInstanceOf[GeoMesaTranslatableTable]
  private val rightDs = rightTable.ds
  private val rightSft = rightTable.getSimpleFeatureType
  private val rightAttributes = {
    val properties = if (right.properties.nonEmpty) right.properties else {
      val attrDescriptors = rightSft.getAttributeDescriptors.asScala
      FID_FIELD_NAME +: attrDescriptors.map(attrDesc => attrDesc.getLocalName).toArray
    }
    rightKeys.map(k => properties(k))
  }

  private val cachedRightResults = mutable.HashMap.empty[AnyRef, Array[AnyRef]]
  private val currentResults = ArrayBuffer.empty[AnyRef]
  private var currentResultIndex = 0
  private var currentResult: AnyRef = _

  override def current(): AnyRef = currentResult

  override def moveNext(): Boolean = {
    if (cancelFlag.get()) false else doMoveNext()
  }

  private def doMoveNext(): Boolean = {
    if (currentResultIndex < currentResults.length) {
      currentResult = currentResults(currentResultIndex)
      currentResultIndex += 1
      true
    } else {
      currentResults.clear()
      fetchNextBatchOfResults()
      if (currentResults.isEmpty) false else {
        currentResultIndex = 0
        moveNext()
      }
    }
  }

  override def reset(): Unit = {
    leftEnumerator.reset()
    currentResults.clear()
    currentResultIndex = 0
    currentResult = null
  }

  override def close(): Unit = {
    leftEnumerator.close()
  }

  private def fetchNextBatchOfResults(): Unit = {
    val leftResultsToLookup = mutable.HashMap.empty[AnyRef, ArrayBuffer[AnyRef]]
    while (currentResults.length < SCAN_BATCH_SIZE && leftResultsToLookup.size < LOOKUP_BATCH_SIZE && leftEnumerator.moveNext()) {
      val leftResult = leftEnumerator.current()
      val equiValues = leftKeysSelector.apply(leftResult)
      cachedRightResults.get(equiValues) match {
        case Some(r) => appendJoinedResults(leftResult, r)
        case None => leftResultsToLookup.getOrElseUpdate(equiValues, ArrayBuffer.empty) += leftResult
      }
    }
    if (leftResultsToLookup.nonEmpty) {
      val rightResultsMap = batchLookupRight(leftResultsToLookup.keysIterator)
      leftResultsToLookup.foreach { case (equiValues, leftResults) =>
        val rightResults = rightResultsMap.get(equiValues) match {
          case Some(r) => r.toArray
          case None => Array.empty[AnyRef]
        }
        cachedRightResults.put(equiValues, rightResults)
        leftResults.foreach(leftResult => appendJoinedResults(leftResult, rightResults))
      }
    }
  }

  private def appendJoinedResults(leftResult: AnyRef, rightResults: Array[AnyRef]): Unit = {
    var hasRightResult = false
    rightResults.foreach { rightResult =>
      if (predicate == null || predicate.apply(leftResult, rightResult)) {
        val result = resultSelector.apply(leftResult, rightResult)
        currentResults += result
        hasRightResult = true
      }
    }
    if (!hasRightResult && generateNullsOnRight) {
      currentResults += resultSelector.apply(leftResult, null)
    }
  }

  private def batchLookupRight(equiValuesSet: Iterator[AnyRef]): mutable.HashMap[AnyRef, ArrayBuffer[AnyRef]] = {
    val query = right.toQuery()
    val filter = query.getFilter
    val extendedFilter = addEquiValuesToGtFilter(filter, equiValuesSet)
    query.setFilter(extendedFilter)
    val rightEnumerator = SimpleFeatureEnumerable.enumerator(rightDs, rightSft, cancelFlag, query, right.properties, 0)
    try {
      val results = mutable.HashMap.empty[AnyRef, ArrayBuffer[AnyRef]]
      while (rightEnumerator.moveNext()) {
        val result = rightEnumerator.current()
        val equiValues = rightKeysSelector.apply(result)
        results.getOrElseUpdate(equiValues, ArrayBuffer.empty) += result
      }
      results
    } finally {
      rightEnumerator.close()
    }
  }

  private def addEquiValuesToGtFilter(filter: Filter, equiValuesSet: Iterator[AnyRef]): Filter = {
    val equiValuesFilters = equiValuesSet.map { equiValues =>
      val attrsWithValues: Seq[(String, AnyRef)] = equiValues match {
        case l: java.util.List[AnyRef] =>
          // We only pickup the first attribute as lookup condition, since compound condition consisting of multiple equi-conds
          // such as ((A0 = V00) AND (A1 = V01)) OR ((A0 = V10) AND (A1 = V11)) OR ((A0 = V20) AND (A1 = V21)) would choke
          // GeoMesa query optimizer's CNF transformation pass.
          Seq((rightAttributes.head, l.get(0)))
        case v => Seq((rightAttributes.head, v))
      }
      val attrFilters = attrsWithValues.map { case (attr, value) =>
        attr match {
          case FID_FIELD_NAME => ff.id(ff.featureId(value.asInstanceOf[String]))
          case _ =>
            val binding = rightSft.getDescriptor(attr).getType.getBinding
            val valueExpr = value match {
              case localTimestamp: java.lang.Long if binding == classOf[Date] || binding == classOf[Timestamp] =>
                ff.literal(convertLocalTimestamp(localTimestamp))
              case _ =>
                ff.literal(value)
            }
            ff.equals(ff.property(attr), valueExpr)
        }
      }
      andFilters(attrFilters)
    }
    val equivValuesFilter = orFilters(equiValuesFilters.toSeq)
    if (filter == Filter.INCLUDE) equivValuesFilter else ff.and(filter, equivValuesFilter)
  }
}

object GeoMesaIndexLookupJoinEnumerator {
  val SCAN_BATCH_SIZE = 100
  val LOOKUP_BATCH_SIZE = 100
  val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2
}
