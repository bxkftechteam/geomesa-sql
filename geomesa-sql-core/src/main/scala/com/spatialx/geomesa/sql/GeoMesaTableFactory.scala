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

import org.apache.calcite.schema.TableFactory
import java.{util => ju}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.schema.SchemaPlus
import org.geotools.data.DataStore

/**
  * Factory that creates a GeoMesaTable instance, which is useful when user
  * want to skip the schema discovery process (which was initiated from
  * GeoMesaSchemaFactory) and access a GeoMesa schema directly.
  */
class GeoMesaTableFactory extends TableFactory[GeoMesaTable] {
  import scala.collection.JavaConverters._

  override def create(schema: SchemaPlus, name: String, operand: ju.Map[String,Object], rowType: RelDataType): GeoMesaTable = {
    val params = operand.asScala.map { case (key, value) => key -> value.toString }
    params.get("geomesa.schema") match {
      case Some(typeName) => 
        val ds = GeoMesaSchemaFactory.getOrCreateDataStore(params.toMap)
        GeoMesaTableFactory.createTable(ds, typeName, params.toMap)
      case None => throw new IllegalArgumentException(s"geomesa.schema was not specified in table factory params: $params")
    }
  }
}

object GeoMesaTableFactory {
  /**
    * Create various types of GeoMesa table instances according to tableType
    * @param ds geotools DataStore object
    * @param typeName table name
    * @param params parameters for configuring the behavior of the table
    * @return GeoMesaTable object
    */
  def createTable(ds: DataStore, typeName: String, params: Map[String, String]): GeoMesaTable = {
    val tableType = params.getOrElse("tableType", "translatable")
    tableType match {
      case "scannable" => new GeoMesaScannableTable(ds, typeName)
      case "translatable" => new GeoMesaTranslatableTable(ds, typeName, params)
      case _ => throw new IllegalStateException(s"invalid table type $tableType")
    }
  }
}
