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

import org.locationtech.jts.geom.Geometry
import org.apache.calcite.runtime.SpatialTypeFunctions
import org.geotools.data.{DataStore, DataStoreFinder, Transaction}
import org.junit.runner.RunWith
import com.spatialx.geomesa.sql.jdbc.Driver.{DATASTORE_CONFIG, DataStoreConfig}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class DriverTest extends Specification {
  import scala.collection.JavaConverters._

  val ds = prepare()
  val jdbcUrl = "jdbc:geomesa:cqengine=true;useGeoIndex=false;caseSensitive=false"

  def prepare(): DataStore = {
    val ds = DataStoreFinder.getDataStore(Map("cqengine" -> "true", "useGeoIndex" -> "false").asJava)
    prepareTestData(ds)
    ds
  }

  def prepareTestData(ds: DataStore): Unit = {
    val spec =
      """I:Integer,
        |S:String,
        |point:Point:srid=4326
    """.stripMargin
    val sft = SimpleFeatureTypes.createType("test_geom_data", spec)
    ds.createSchema(sft)
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      for (i <- 0 to 10) {
        for (j <- 0 to 10) {
          val sf = new ScalaSimpleFeature(sft, s"#$i-$j")
          sf.setAttribute("I", Int.box(i + j))
          sf.setAttribute("S", s"str_${i}_$j")
          sf.setAttribute("point", s"POINT($i $j)")
          FeatureUtils.write(writer, sf)
        }
      }
    }
  }

  def runSql(sql: String): Array[Array[AnyRef]] = {
    var connection: Connection = null
    var statement: Statement = null
    var resultSet: ResultSet = null
    try {
      connection = DriverManager.getConnection(jdbcUrl)
      statement = connection.createStatement()
      resultSet = statement.executeQuery(sql)
      val columnCount = resultSet.getMetaData.getColumnCount
      val resultArray = ArrayBuffer.empty[Array[AnyRef]]
      while (resultSet.next()) {
        val line = Range(0, columnCount).map(k => resultSet.getObject(k + 1))
        resultArray.append(line.toArray)
      }
      resultArray.toArray
    } finally {
      Option(resultSet).foreach(_.close())
      Option(statement).foreach(_.close())
      Option(connection).foreach(_.close())
    }
  }

  "JDBC Driver" should {
    "handle non geom queries correctly" in {
      val result = runSql("SELECT * FROM test_geom_data WHERE I < 10")
      result must not(beEmpty)
    }

    "handle geom queries correctly" in {
      val result = runSql("SELECT I, point, ST_AsText(point) FROM test_geom_data WHERE ST_Contains(ST_MakeEnvelope(3, 3, 6, 6), point)")
      result must not(beEmpty)
      result(0) must haveLength(3)
      result(0)(1) must beAnInstanceOf[Geometry]
      val g = result(0)(1).asInstanceOf[Geometry]
      val wkt = result(0)(2).asInstanceOf[String]
      SpatialTypeFunctions.ST_AsWKT(g) mustEqual wkt
    }

    "building jdbc connection using complex datasource config" in {
      var connection: Connection = null
      var statement: Statement = null
      var resultSet: ResultSet = null
      try {
        val info = new Properties()
        val dataStoreConfigs = Seq(
          DataStoreConfig("DB_A", Map("cqengine" -> "true", "useGeoIndex" -> "false").asJava),
          DataStoreConfig("DB_B", Map("cqengine" -> "true", "useGeoIndex" -> "false").asJava)
        ).asJava
        info.put(DATASTORE_CONFIG, dataStoreConfigs)
        connection = DriverManager.getConnection("jdbc:geomesa:caseSensitive=false", info)
        statement = connection.createStatement()
        resultSet = statement.executeQuery("SELECT * FROM db_a.test_geom_data a, db_b.test_geom_data b WHERE a.S = b.S")
        val columnCount = resultSet.getMetaData.getColumnCount
        var count = 0
        while (resultSet.next()) {
          count += 1
        }
        columnCount must beGreaterThan(0)
        count must beGreaterThan(0)
      } finally {
        Option(resultSet).foreach(_.close())
        Option(statement).foreach(_.close())
        Option(connection).foreach(_.close())
      }
    }

    "getting result set metadata without running query" in {
      var connection: Connection = null
      var statement: PreparedStatement = null
      try {
        connection = DriverManager.getConnection(jdbcUrl)
        statement = connection.prepareStatement("SELECT I, point, ST_AsText(point) FROM test_geom_data WHERE ST_Contains(ST_MakeEnvelope(3, 3, 6, 6), point)")
        val meta = statement.getMetaData
        meta.getColumnCount mustEqual 3
        meta.getColumnTypeName(1) mustEqual "INTEGER"
        meta.getColumnTypeName(2) mustEqual "GEOMETRY"
        meta.getColumnTypeName(3) mustEqual "VARCHAR"
      } finally {
        Option(statement).foreach(_.close())
        Option(connection).foreach(_.close())
      }
    }
  }
}
