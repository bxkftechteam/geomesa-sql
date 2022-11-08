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
import org.geotools.data.{DataStore, FeatureReader, Query, Transaction}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.util.concurrent.atomic.AtomicBoolean

/**
  * Enumerator for iterating through simple features stored in geotools
  * DataStore, and apply converter to simple features to retrieve converted row
  * objects of type T.
  */
class SimpleFeatureEnumerator[T <: AnyRef] (
  val ds: DataStore, val query: Query, val offset: Long, val cancelFlag: AtomicBoolean,
  val converter: SimpleFeatureConverter[T]) extends Enumerator[T] {
  private var featureReader: FeatureReader[SimpleFeatureType, SimpleFeature] = createFeatureReader()
  private var sfOpt: Option[SimpleFeature] = None

  override def current(): T = {
    sfOpt match {
      case Some(sf) => converter.convert(sf)
      case None => null.asInstanceOf[T]
    }
  }

  override def moveNext(): Boolean = {
    if (cancelFlag.get()) false else {
      sfOpt = if (featureReader.hasNext) Some(featureReader.next()) else None
      sfOpt.isDefined
    }
  }

  override def reset(): Unit = {
    close()
    featureReader = createFeatureReader()
  }

  override def close(): Unit = {
    if (featureReader != null) {
      featureReader.close()
      featureReader = null
    }
    sfOpt = None
  }

  private def createFeatureReader() = {
    val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

    // Skip the offset
    var x = 0L
    while (x < offset) {
      if (reader.hasNext) {
        reader.next()
        x += 1
      } else {
        x = offset // break
      }
    }

    reader
  }
}
