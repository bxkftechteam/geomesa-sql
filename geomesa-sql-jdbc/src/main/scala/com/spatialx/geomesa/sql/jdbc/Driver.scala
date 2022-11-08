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

package com.spatialx.geomesa.sql.jdbc

import org.apache.calcite.avatica.{AvaticaConnection, Handler, HandlerImpl}
import org.apache.calcite.jdbc.{CalciteConnection, CalcitePrepare}
import org.apache.calcite.linq4j.function
import org.apache.calcite.model.ModelHandler
import org.apache.calcite.util.{JsonBuilder, Util}
import com.spatialx.geomesa.sql.GeoMesaSchemaFactory
import com.spatialx.geomesa.sql.jdbc.Driver.{DATASTORE_CONFIG, DataStoreConfig, isRegistered}

import java.io.IOException
import java.sql.SQLException
import java.util
import java.util.Properties

/**
 * JDBC driver for GeoMesa DataStores, which is a thin wrapper around Calcite JDBC driver.
 *
 * Simply use DriverManager.getConnection(jdbcUrl) to get a connection for querying geomesa datastore using SQL,
 * where jdbcUrl is "jdbc:geomesa:<prop0>=<value0>;<prop1>=<value1>;...". Property names and values specified in
 * jdbcUrl is identical with the properties for connecting to GeoMesa DataStore.
 */
class Driver extends org.apache.calcite.jdbc.Driver {
  import scala.collection.JavaConverters._

  override def createPrepareFactory(): function.Function0[CalcitePrepare] = {
    () => new GeoMesaPrepareImpl
  }

  override def getConnectStringPrefix: String = "jdbc:geomesa:"

  override def createHandler(): Handler = new HandlerImpl {
    override def onConnectionInit(connection_ : AvaticaConnection): Unit = {
      val connection = connection_.asInstanceOf[CalciteConnection]
      super.onConnectionInit(connection_)

      // Enable calcite spatial functions
      var fun = connection.getProperties.getProperty("fun", "spatial")
      if (!fun.contains("spatial")) {
        fun += ",spatial"
      }
      connection.getProperties.put("fun", fun)

      // Build calcite custom schema for geomesa, which was implemented by GeoMesaSchemaFactory
      val model = buildCalciteModel(connection)
      try new ModelHandler(connection, model)
      catch {
        case e: IOException => throw new SQLException(e)
      }
    }

    private def buildCalciteModel(connection: CalciteConnection): String = {
      val properties = connection.getProperties
      val schemaName = Util.first(connection.config.schema, "GEOMESA")
      val json = new JsonBuilder
      val root = json.map
      root.put("version", "1.0")
      root.put("defaultSchema", schemaName)
      val schemaList = json.list
      root.put("schemas", schemaList)
      val dataStoreConfigs = connection.getProperties.get(DATASTORE_CONFIG).asInstanceOf[java.util.List[DataStoreConfig]]
      if (dataStoreConfigs != null && !dataStoreConfigs.isEmpty) {
        dataStoreConfigs.forEach(dataStoreConfig => {
          val schema = buildSchema(json, dataStoreConfig)
          schemaList.add(schema)
        })
      } else {
        val schema = buildSchema(json, schemaName, properties)
        schemaList.add(schema)
      }
      "inline:" + json.toJsonString(root)
    }

    private def buildSchema(json: JsonBuilder, schemaName: String, properties: Properties): util.Map[String, AnyRef] = {
      val schema = json.map
      schema.put("type", "custom")
      schema.put("name", schemaName)
      schema.put("factory", classOf[GeoMesaSchemaFactory].getName)
      val operandMap = json.map
      schema.put("operand", operandMap)
      properties.entrySet().forEach(entry => {
        operandMap.put(entry.getKey.toString, entry.getValue)
      })
      schema
    }

    private def buildSchema(json: JsonBuilder, dataStoreConfig: DataStoreConfig): util.Map[String, AnyRef] = {
      val schema = json.map
      schema.put("type", "custom")
      schema.put("name", dataStoreConfig.schemaName)
      schema.put("factory", classOf[GeoMesaSchemaFactory].getName)
      val operandMap = json.map
      schema.put("operand", operandMap)
      dataStoreConfig.params.asScala.foreach { case (key, value) => operandMap.put(key, value) }
      schema
    }
  }

  /**
   * Ensure that the driver will be registered on first instantiation of Driver object.
   */
  isRegistered.synchronized {
    if (!isRegistered) {
      register()
      isRegistered = true
    }
  }
}

object Driver {
  /**
   * Schema config binds schema names to geomesa datastores
   * @param schemaName schema name
   * @param params params for connecting to geomesa datastore
   */
  case class DataStoreConfig(
    schemaName: String,
    params: java.util.Map[String, String]
  )

  /**
   * Specify a java.util.List of DataStoreConfig for DATASTORE_CONFIG property when getting JDBC connection
   * to run queries spanning multiple geomesa datastores.
   */
  val DATASTORE_CONFIG: String = "geomesa.datastores"

  var isRegistered: Boolean = false
}
