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

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.schema.ScannableTable
import org.geotools.data.{DataStore, Query}
import com.spatialx.geomesa.sql.enumerator.{SimpleFeatureArrayConverter, SimpleFeatureEnumerator}
import org.opengis.filter.Filter

import java.util.concurrent.atomic.AtomicBoolean

/**
  * ScannableTable implementation only support scanning data sequentially from
  * DataStore, which does not take advantage of GeoMesa indexes. This is a
  * baseline implementation for verifying the correctness of other more
  * optimized implementations of Table interface.
  */
class GeoMesaScannableTable(ds: DataStore, typeName: String) extends GeoMesaTable(ds, typeName) with ScannableTable {
  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = {
    val typeFactory = root.getTypeFactory
    val fieldTypes = getFieldTypes(typeFactory)
    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get[AtomicBoolean](root)
    new AbstractEnumerable[Array[AnyRef]]() {
      override def enumerator(): Enumerator[Array[AnyRef]] = {
        val converter = new SimpleFeatureArrayConverter[Array[AnyRef]](fieldTypes.size)
        new SimpleFeatureEnumerator(ds, new Query(typeName, Filter.INCLUDE), 0L, cancelFlag, converter)
      }
    }
  }
}
