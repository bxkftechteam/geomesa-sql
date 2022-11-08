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
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.geotools.data.DataStore

import java.util.concurrent.TimeUnit
import java.{util => ju}

/**
  * Schema for discovering simple feature types stored in GeoTools DataStores,
  * where SimpleFeatureTypes in DataStores were populated as Tables.
  */
class GeoMesaSchema(val ds: DataStore, val params: Map[String, String]) extends AbstractSchema {
  import scala.collection.JavaConverters._

  private val schemaCacheExpireSeconds = params.getOrElse("schemaCacheExpireSeconds", "600").toInt

  /**
    * Get a map of all tables in this schema, a.k.a all simple feature types in
    * this DataStore.
    */
  override def getTableMap: ju.Map[String, Table] = {
    if (schemaCacheExpireSeconds > 0) tableMapCache.get(0) else populateTableMap()
  }

  private def createTable(typeName: String): Table = {
    GeoMesaTableFactory.createTable(ds, typeName, params)
  }

  private def populateTableMap(): ju.Map[String, Table] = {
    val typeNames = ds.getTypeNames
    val tableMap = typeNames.map(typeName => typeName -> createTable(typeName)).toMap
    tableMap.asJava
  }

  private val tableMapCache = Caffeine.newBuilder()
    .expireAfterWrite(schemaCacheExpireSeconds, TimeUnit.SECONDS)
    .build[Integer, ju.Map[String, Table]](new CacheLoader[Integer, ju.Map[String, Table]] {
      override def load(key: Integer): ju.Map[String, Table] = populateTableMap()
    })
}
