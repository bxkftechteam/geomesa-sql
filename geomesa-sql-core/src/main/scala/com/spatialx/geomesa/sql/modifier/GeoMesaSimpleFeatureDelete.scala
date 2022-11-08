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

package com.spatialx.geomesa.sql.modifier

import org.geotools.data.{DataStore, Transaction}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer

/**
 * Delete simple features from GeoMesa table
 */
class GeoMesaSimpleFeatureDelete(ds: DataStore, typeName: String) extends GeoMesaSimpleFeatureModifier {
  import scala.collection.JavaConverters._

  private val bufferedFeatureIds = ArrayBuffer.empty[String]
  private val ff = CommonFactoryFinder.getFilterFactory2

  override def modify(sf: SimpleFeature): Unit = {
    bufferedFeatureIds += sf.getID
  }

  override def flush(): Long = {
    val fids = bufferedFeatureIds.map(ff.featureId)
    if (fids.isEmpty) 0L else {
      val filter = ff.id(fids.toSet.asJava)
      var count = 0L
      WithClose(ds.getFeatureWriter(typeName, filter, Transaction.AUTO_COMMIT)) { writer =>
        while (writer.hasNext) {
          writer.next()
          writer.remove()
          count += 1L
        }
      }
      bufferedFeatureIds.clear()
      count
    }
  }

  override def close(): Unit = {
    bufferedFeatureIds.clear()
  }
}
