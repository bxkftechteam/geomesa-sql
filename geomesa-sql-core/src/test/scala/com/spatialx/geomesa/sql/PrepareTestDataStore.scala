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

import org.geotools.data.{DataStore, DataStoreFinder, Transaction}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.FileInputStream
import java.util.{Date, TimeZone, UUID}
import org.apache.calcite.util.JsonBuilder

/**
 * Create a DataStore and load some data for testing
 */
object PrepareTestDataStore {
  import scala.collection.JavaConverters._

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  System.setProperty("geomesa.sft.config.urls", getClass.getClassLoader.getResource("test_data.conf").toString)
  System.setProperty("geomesa.convert.config.urls", getClass.getClassLoader.getResource("test_data.conf").toString)

  val catalog = s"${MiniCluster.namespace}.ut"
  val ds: DataStore = prepare()
  val model: String = buildInlineModel("translatable")
  val scannableModel: String = buildInlineModel("scannable")
  val tableModel: String = buildInlineTableModel("translatable", "test_data")

  def prepareTestData(ds: DataStore): Unit = {
    val sft = SimpleFeatureTypeLoader.sftForName("test_data").get
    val conf = ConverterConfigLoader.configForName("test_data").get
    val converter = SimpleFeatureConverter(sft, conf)
    ds.createSchema(sft)
    val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
    try {
      val is = new FileInputStream(getClass.getClassLoader.getResource("test_data_geomesa.csv").getFile)
      WithClose(converter.process(is)) { features =>
        features.foreach { feature => FeatureUtils.write(writer, feature) }
      }
    } finally {
      converter.close()
      writer.close()
    }
  }

  def prepareTestNullData(ds: DataStore): Unit = {
    val spec =
      """I:Integer,
        |L:Long,
        |F:Float,
        |D:Double,
        |U:UUID,
        |S:String,
        |B:Boolean,
        |TS:Date,
        |bytes:Bytes
    """.stripMargin
    val sft = SimpleFeatureTypes.createType("test_null_data", spec)
    ds.createSchema(sft)
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      val sf = new ScalaSimpleFeature(sft, "0")
      FeatureUtils.write(writer, sf)
    }
  }

  def prepareTestComplexData(ds: DataStore): Unit = {
    val spec = "bytes:Bytes"
    val sft = SimpleFeatureTypes.createType("test_complex_data", spec)
    ds.createSchema(sft)
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      val sf = new ScalaSimpleFeature(sft, "0")
      sf.setAttribute("bytes", Array[Byte](0, 1, 2, 3, 4))
      FeatureUtils.write(writer, sf)
    }
  }

  def prepareTestGeomData(ds: DataStore): Unit = {
    val spec =
      """I:Integer,
        |pt:Point:srid=4326,
        |line:LineString:srid=4326,
        |poly:Polygon:srid=4326,
        |points:MultiPoint:srid=4326,
        |lines:MultiLineString:srid=4326,
        |polys:MultiPolygon:srid=4326,
        |geoms:GeometryCollection:srid=4326
    """.stripMargin
    val sft = SimpleFeatureTypes.createType("test_geom_data", spec)
    ds.createSchema(sft)
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      for (i <- 0 to 10) {
        for (j <- 0 to 10) {
          val sf = new ScalaSimpleFeature(sft, s"#$i-$j")
          sf.setAttribute("I", Int.box(i + j))
          sf.setAttribute("pt", s"POINT($i $j)")
          sf.setAttribute("line", "LINESTRING(0 2, 2 0, 8 6)")
          sf.setAttribute("poly", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
          sf.setAttribute("points", "MULTIPOINT(0 0, 2 2)")
          sf.setAttribute("lines", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
          sf.setAttribute("polys", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))")
          sf.setAttribute("geoms", "GEOMETRYCOLLECTION(POINT(45.0 49.0),POINT(45.1 49.1))")
          FeatureUtils.write(writer, sf)
        }
      }
    }
  }

  def prepareTestGeomDataSimple(ds: DataStore): Unit = {
    val spec =
      """I:Integer,
        |pt:Point:srid=4326,
        |line:LineString:srid=4326,
        |poly:Polygon:srid=4326
    """.stripMargin
    val sft = SimpleFeatureTypes.createType("test_geom_data_simple", spec)
    ds.createSchema(sft)
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      for (i <- 0 to 10) {
        val sf = new ScalaSimpleFeature(sft, s"#$i")
        val min = i - 0.2
        val max = i + 0.2
        sf.setAttribute("I", Int.box(i))
        sf.setAttribute("pt", s"POINT($max $max)")
        sf.setAttribute("line", s"LINESTRING($min $min, $max $max)")
        sf.setAttribute("poly", s"POLYGON(($min $min, $min $max, $max $max, $max $min, $min $min))")
        FeatureUtils.write(writer, sf)
      }
    }
  }

  def prepareTestDataForModification(ds: DataStore, typeName: String, rows: Int): Unit = {
    val spec =
      """I:Integer,
        |L:Long,
        |F:Float,
        |D:Double,
        |U:UUID,
        |S:String,
        |B:Boolean,
        |TS:Date,
        |bytes:Bytes,
        |geom:Point:srid=4326
    """.stripMargin
    val sft = SimpleFeatureTypes.createType(typeName, spec)
    ds.createSchema(sft)
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      for (i <- 0 until rows) {
        val fid = s"#$i"
        val sf = new ScalaSimpleFeature(sft, fid)
        sf.setAttribute("I", Int.box(i))
        sf.setAttribute("L", Long.box(i + 10000000000L))
        sf.setAttribute("F", Float.box(i * 0.1f))
        sf.setAttribute("D", Double.box(i * 0.01))
        sf.setAttribute("U", UUID.fromString(f"00000000-0000-0000-0000-000000000$i%03d"))
        sf.setAttribute("S", f"str$i%03d")
        sf.setAttribute("B", Boolean.box(i % 2 == 0))
        sf.setAttribute("TS", new Date(1600000000000L + i * 1000L))
        sf.setAttribute("bytes", Array[Byte](0, 1, 2, 3))
        sf.setAttribute("geom", s"POINT(${i % 90} ${(i + 1) % 90})")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.getUserData.put(Hints.PROVIDED_FID, fid)
        FeatureUtils.write(writer, sf)
      }
    }
  }

  def prepare(): DataStore = {
    val ds = DataStoreFinder.getDataStore(Map(
      "accumulo.instance.id" -> MiniCluster.cluster.getInstanceName,
      "accumulo.zookeepers" -> MiniCluster.cluster.getZooKeepers,
      "accumulo.user" -> MiniCluster.Users.root.name,
      "accumulo.password" -> MiniCluster.Users.root.password,
      "accumulo.catalog" -> catalog
    ).asJava)
    prepareTestData(ds)
    prepareTestNullData(ds)
    prepareTestComplexData(ds)
    prepareTestGeomData(ds)
    prepareTestGeomDataSimple(ds)
    ds
  }

  private def buildInlineModel(tableType: String): String = {
    val json = new JsonBuilder
    val root = json.map
    root.put("version", "1.0")
    root.put("defaultSchema", "GEOMESA")
    val schemaList = json.list
    root.put("schemas", schemaList)
    val schema = json.map
    schemaList.add(schema)
    schema.put("type", "custom")
    schema.put("name", "GEOMESA")
    schema.put("factory", classOf[GeoMesaSchemaFactory].getName)
    val operandMap = buildOperandMap(json, tableType)
    schema.put("operand", operandMap)
    "inline:" + json.toJsonString(root)
  }

  private def buildInlineTableModel(tableType: String, tableName: String): String = {
    val json = new JsonBuilder
    val root = json.map
    root.put("version", "1.0")
    root.put("defaultSchema", "GEOMESA")
    val schemaList = json.list
    root.put("schemas", schemaList)
    val schema = json.map
    schemaList.add(schema)
    schema.put("name", "GEOMESA")
    val tableList = json.list
    schema.put("tables", tableList)
    val table = json.map
    tableList.add(table)
    table.put("name", tableName.toUpperCase)
    table.put("type", "custom")
    table.put("factory", classOf[GeoMesaTableFactory].getName)
    val operandMap = buildOperandMap(json, tableType)
    operandMap.put("geomesa.schema", tableName)
    table.put("operand", operandMap)
    "inline:" + json.toJsonString(root)
  }

  private def buildOperandMap(json: JsonBuilder, tableType: String): java.util.Map[String, AnyRef] = {
    val operandMap = json.map
    operandMap.put("tableType", tableType)
    operandMap.put("schemaCacheExpireSeconds", "0")
    operandMap.put("accumulo.instance.id", MiniCluster.cluster.getInstanceName)
    operandMap.put("accumulo.zookeepers", MiniCluster.cluster.getZooKeepers)
    operandMap.put("accumulo.user", MiniCluster.Users.root.name)
    operandMap.put("accumulo.password", MiniCluster.Users.root.password)
    operandMap.put("accumulo.catalog", catalog)
    operandMap
  }
}
