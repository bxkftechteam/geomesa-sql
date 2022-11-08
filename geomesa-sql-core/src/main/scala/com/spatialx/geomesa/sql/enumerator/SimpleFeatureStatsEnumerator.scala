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

import org.apache.calcite.linq4j.Enumerator
import org.geotools.data.{DataStore, Query, Transaction}
import com.spatialx.geomesa.sql.enumerator.AttributeConverter.convert
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.utils.stats.{CountStat, GroupBy, MinMax, SeqStat, Stat}
import org.opengis.feature.simple.SimpleFeatureType

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Enumerator for result sets of GeoMesa stats queries
 */
class SimpleFeatureStatsEnumerator(
  val ds: DataStore, val sft: SimpleFeatureType, val query: Query,
  val statsStrings: Seq[String], statAttributes: Seq[String],
  val offset: Long, val cancelFlag: AtomicBoolean)
  extends Enumerator[AnyRef] {

  private val isGrouped = statsStrings.head.startsWith("GroupBy")
  private val groupStatsTable: mutable.HashMap[AnyRef, ArrayBuffer[AnyRef]] = mutable.HashMap.empty
  private val nonGroupStats: mutable.ArrayBuffer[AnyRef] = new mutable.ArrayBuffer(statAttributes.length)
  private val rows: Array[AnyRef] = readStats()
  private var cursor: Int = offset.toInt
  private val limit = query.getMaxFeatures
  private var currentRow: AnyRef = _

  override def current(): AnyRef = currentRow

  override def moveNext(): Boolean = {
    // XXX: cancelFlag won't work for most of the cases, since aggregation will be performed by
    // GeoMesa DataStore can be cannot interrupt it when query was running synchronously.
    if (cancelFlag.get()) false else doMoveNext()
  }

  def doMoveNext(): Boolean = {
    if (cursor < rows.length && cursor < limit) {
      currentRow = rows(cursor)
      cursor += 1
      true
    } else false
  }

  override def reset(): Unit = {
    cursor = offset.toInt
  }

  override def close(): Unit = ()

  /**
    * Fetch all stats results and organize it as a table
    */
  private def readStats(): Array[AnyRef] = {
    val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    try {
      val stat = StatsScan.decodeStat(sft)(reader.next.getAttribute(0).asInstanceOf[String])
      parseStat(stat, statAttributes.head)
      if (isGrouped) {
        groupStatsTable.map { case (groupKey, aggrValueArray) =>
          (convert(groupKey) +: aggrValueArray).toArray
        }.toArray
      } else {
        if (nonGroupStats.length > 1) Array(nonGroupStats.toArray) else nonGroupStats.toArray
      }
    } finally {
      reader.close()
    }
  }

  private def parseGroupedStat(stat: GroupBy[_], attribute: String): Unit = {
    val rowCapacity = statAttributes.length
    stat.iterator.foreach { case (groupKey, aggrStat)  =>
      val aggrValueArray = groupStatsTable.getOrElseUpdate(groupKey.asInstanceOf[AnyRef], new ArrayBuffer[AnyRef](rowCapacity))
      val aggrValue = aggrStat match {
        case countStat: CountStat => Long.box(countStat.count)
        case minMax: MinMax[_] => convert((if (attribute == "min") minMax.min else minMax.max).asInstanceOf[AnyRef])
      }
      aggrValueArray += aggrValue
    }
  }

  private def parseSeqStat(stat: SeqStat): Unit = {
    stat.stats.zip(statAttributes).foreach { case (s, attribute) =>
      parseStat(s, attribute)
    }
  }

  private def parseStat(stat: Stat, attribute: String): Unit = {
    stat match {
      case s: SeqStat => parseSeqStat(s)
      case s: GroupBy[_] => parseGroupedStat(s, attribute)
      case s: CountStat => nonGroupStats += Long.box(s.count)
      case s: MinMax[_] => nonGroupStats += convert((if (attribute == "min") s.min else s.max).asInstanceOf[AnyRef])
      case _ => throw new NotImplementedError(s"GeoMesa Stat type ${stat.getClass.getCanonicalName} is not supported yet")
    }
  }
}
