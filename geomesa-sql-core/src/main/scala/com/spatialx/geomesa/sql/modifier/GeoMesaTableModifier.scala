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

import org.apache.calcite.linq4j.Enumerable
import com.spatialx.geomesa.sql.GeoMesaTranslatableTable
import com.spatialx.geomesa.sql.enumerator.AttributeConverter.convert
import com.spatialx.geomesa.sql.modifier.GeoMesaTableModifier.{MODIFY_BATCH_SIZE, TableModifyOperationType}
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer

/**
 * Modifier for inserting into, updating and deleting simple features from GeoMesa table
 */
class GeoMesaTableModifier(cancelFlag: AtomicBoolean, geomesaTable: GeoMesaTranslatableTable,
                           source: Enumerable[AnyRef], opType: Int) {

  private val ds = geomesaTable.ds
  private val typeName = geomesaTable.typeName

  private val sfModifier = opType match {
    case TableModifyOperationType.INSERT => new GeoMesaSimpleFeatureInsert(ds, typeName)
    case TableModifyOperationType.UPDATE => new GeoMesaSimpleFeatureUpdate(ds, typeName)
    case TableModifyOperationType.DELETE => new GeoMesaSimpleFeatureDelete(ds, typeName)
    case _ => throw new IllegalArgumentException(s"invalid opType: $opType")
  }

  def modifyTable(): Long = {
    val sft = geomesaTable.getSimpleFeatureType
    val enumerator = source.enumerator()
    try {
      var cnt = 0
      var affectedRows = 0L
      while (!cancelFlag.get() && enumerator.moveNext()) {
        val row = enumerator.current().asInstanceOf[Array[AnyRef]]
        val sf = convertRowToSimpleFeature(sft, row)
        sfModifier.modify(sf)
        cnt += 1
        if (cnt >= MODIFY_BATCH_SIZE) {
          affectedRows += sfModifier.flush()
          cnt = 0
        }
      }
      if (cnt > 0) {
        affectedRows += sfModifier.flush()
      }
      affectedRows
    } finally {
      enumerator.close()
    }
  }

  def close(): Unit = {
    sfModifier.close()
  }

  private def convertRowToSimpleFeature(sft: SimpleFeatureType, row: Array[AnyRef]): SimpleFeature = {
    val fid = row(0).asInstanceOf[String]
    val rowLength = row.length
    val attrTypes = sft.getTypes
    val attrValues = new ArrayBuffer[AnyRef](rowLength - 1)
    var k = 1
    while (k < rowLength) {
      val value = row(k)
      val attrValue = if (value == null) value else {
        val binding = attrTypes.get(k - 1).getBinding
        convert(binding, value)
      }
      attrValues += attrValue
      k += 1
    }
    ScalaSimpleFeatureFactory.buildFeature(sft, attrValues, fid)
  }
}

object GeoMesaTableModifier {
  val MODIFY_BATCH_SIZE = 100

  object TableModifyOperationType {
    val INSERT = 1
    val UPDATE = 2
    val DELETE = 3
  }
}
