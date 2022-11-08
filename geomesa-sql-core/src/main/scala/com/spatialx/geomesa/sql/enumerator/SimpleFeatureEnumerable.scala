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

import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerator}
import org.geotools.data.{DataStore, Query}
import com.spatialx.geomesa.sql.{FID_FIELD_NAME, GeoMesaQueryParams}
import org.opengis.feature.simple.SimpleFeatureType

import java.util.concurrent.atomic.AtomicBoolean

/**
  * Enumerable instance generates an enumerator instance to produces the result
  * set.
  */
class SimpleFeatureEnumerable(
  ds: DataStore, sft: SimpleFeatureType, cancelFlag: AtomicBoolean, queryParams: GeoMesaQueryParams)
    extends AbstractEnumerable[AnyRef] {
  override def enumerator(): Enumerator[AnyRef] = {
    val query = queryParams.toQuery()
    val offset = queryParams.offset
    if (queryParams.statsStrings.nonEmpty) {
      new SimpleFeatureStatsEnumerator(ds, sft, query, queryParams.statsStrings, queryParams.statAttributes, offset, cancelFlag)
    } else {
      SimpleFeatureEnumerable.enumerator(ds, sft, cancelFlag, query, queryParams.properties, offset)
    }
  }
}

object SimpleFeatureEnumerable {
  def enumerator(ds: DataStore, sft: SimpleFeatureType, cancelFlag: AtomicBoolean,
                 query: Query, properties: Array[String], offset: Long): Enumerator[AnyRef] = {
    val converter = if (properties.nonEmpty) {
      val propertiesWithoutFid = properties.filter(_ != FID_FIELD_NAME)
      query.setPropertyNames(propertiesWithoutFid)
      val fieldIndices = properties.map { propName =>
        if (propName == FID_FIELD_NAME) 0 else propertiesWithoutFid.indexOf(propName) + 1
      }
      if (fieldIndices.length == 1) new SimpleFeatureValueConverter(fieldIndices(0))
      else new SimpleFeatureArrayConverter(fieldIndices)
    } else {
      val numFields = sft.getAttributeCount + 1
      new SimpleFeatureArrayConverter(numFields)
    }
    new SimpleFeatureEnumerator(ds, query, offset, cancelFlag, converter)
  }
}
