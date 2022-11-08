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

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.calcite.schema.{Schema, SchemaFactory, SchemaPlus}
import org.geotools.data.{DataStore, DataStoreFinder}
import com.spatialx.geomesa.sql.GeoMesaSchemaFactory.{getOrCreateDataStore, schemaCache}

import java.{util => ju}

/**
  * Factory for instantiating a Schema object for accessing simple feature
  * types in GeoMesa datastore. This is the entrypoint for discovering tables
  * and rows in GeoMesa datastore.
  */
class GeoMesaSchemaFactory extends SchemaFactory {
  import scala.collection.JavaConverters._

  override def create(parentSchema: SchemaPlus, name: String, operand: ju.Map[String, AnyRef]): Schema = {
    val params = operand.asScala.map { case (key, value) => key -> value.toString }.toMap
    schemaCache.get(params)
  }
}

object GeoMesaSchemaFactory {
  import scala.collection.JavaConverters._

  /**
    * Create a DataStore object using params or get an already created
    * DataStore object. DataStore objects were permanently cached for faster
    * schema discovery.
    */
  def getOrCreateDataStore(params: Map[String, String]): DataStore = dataStoreCache.get(params)

  private val dataStoreCache = Caffeine.newBuilder().build[Map[String, String], DataStore](
    new CacheLoader[Map[String, String], DataStore] {
      override def load(key: Map[String, String]): DataStore = 
        Option(DataStoreFinder.getDataStore(key.asJava)) match {
          case Some(ds) => ds
          case None => throw new RuntimeException(s"DataStore not found for $key")
        }
    }
  )

  private val schemaCache = Caffeine.newBuilder().build[Map[String, String], Schema](
    new CacheLoader[Map[String, String], Schema] {
      override def load(params: Map[String, String]): Schema = {
        val ds = getOrCreateDataStore(params)
        new GeoMesaSchema(ds, params)
      }
    }
  )
}
