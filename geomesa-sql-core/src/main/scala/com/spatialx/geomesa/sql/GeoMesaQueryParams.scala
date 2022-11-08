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

package com.spatialx.geomesa.sql

import org.apache.calcite.linq4j.tree.{Expression, Expressions}
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.filter.Filter

/**
 * Parameters for querying GeoMesa datastore
 */
case class GeoMesaQueryParams(
  typeName: String,
  ecql: String = "",
  properties: Array[String] = Array.empty,
  statsStrings: Array[String] = Array.empty,
  statAttributes: Array[String] = Array.empty,
  offset: Long = 0,
  fetch: Long = 0) {

  import scala.collection.JavaConverters._

  def toExpression(): Expression = {
    Expressions.new_(
      classOf[GeoMesaQueryParams],
      Expressions.constant(typeName),
      Expressions.constant(ecql),
      Expressions.newArrayInit(classOf[String], properties.map(Expressions.constant(_)).toList.asJava),
      Expressions.newArrayInit(classOf[String], statsStrings.map(Expressions.constant(_)).toList.asJava),
      Expressions.newArrayInit(classOf[String], statAttributes.map(Expressions.constant(_)).toList.asJava),
      Expressions.constant(offset),
      Expressions.constant(fetch))
  }

  def toQuery(): Query = {
    val filter = if (ecql.nonEmpty) ECQL.toFilter(ecql) else Filter.INCLUDE
    val query = new Query(typeName, filter)
    if (fetch > 0) query.setMaxFeatures((offset + fetch).toInt)
    query.getHints.put(QueryHints.LOOSE_BBOX, false)
    if (statsStrings.nonEmpty) {
      query.getHints.put(QueryHints.STATS_STRING, Stat.SeqStat(statsStrings))
      query.getHints.put(QueryHints.ENCODE_STATS, true)
    }
    query
  }
}
