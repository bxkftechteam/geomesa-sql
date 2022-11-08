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

import org.geotools.data.DataStore
import org.junit.runner.RunWith
import com.spatialx.geomesa.sql.GeoMesaCalciteTestUtils.{checkSql, verifyResult}
import org.specs2.execute.Result
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Date

/**
 * Testing retrieving values of various types using GeoMesa Calcite
 */
@RunWith(classOf[JUnitRunner])
class DataTypeTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds

  "GeoMesa Calcite Adapter" should {
    "table factory works correctly" in {
      verifyResult("SELECT * FROM TEST_DATA", PrepareTestDataStore.tableModel, "model-csv.yaml")
    }

    "correctly handle non-geometry types" in {
      verifyResult("SELECT * FROM TEST_DATA", PrepareTestDataStore.model, "model-csv.yaml")
    }

    "object of non-geometry types has correct java type" in {
      checkSql("SELECT * FROM TEST_DATA LIMIT 1", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(1)
        val row = resultArray.head
        row must haveLength(9)
        row(0) must beAnInstanceOf[String]
        row(1) must beAnInstanceOf[java.lang.Integer]
        row(2) must beAnInstanceOf[java.lang.Long]
        row(3) must beAnInstanceOf[java.lang.Float]
        row(4) must beAnInstanceOf[java.lang.Double]
        row(5) must beAnInstanceOf[java.lang.Boolean]
        row(6) must beAnInstanceOf[String]
        row(7) must beAnInstanceOf[String]
        row(8) must beAnInstanceOf[Date]
      }
    }

    "correctly handle complex types" in {
      checkSql("SELECT * FROM TEST_COMPLEX_DATA LIMIT 1", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(1)
        val row = resultArray.head
        row(1) must beAnInstanceOf[Array[_]]
        row(1) mustEqual Array(0, 1, 2, 3, 4)
      }
    }

    "correctly handle geometry types" in {
      checkSql("SELECT * FROM TEST_GEOM_DATA LIMIT 1", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(1)
        val row = resultArray.head
        row must haveLength(9)
      } and checkSql("SELECT ST_AsWKT(pt), ST_AsWKT(line), ST_AsWKT(poly), ST_AsWKT(points), ST_AsWKT(lines), ST_AsWKT(polys), ST_AsWKT(geoms) " +
        "FROM TEST_GEOM_DATA LIMIT 1", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(1)
        val row = resultArray.head
        row(0).toString must startWith("POINT")
        row(1).toString must contain("LINE")
        row(2).toString must contain("POLYGON")
        row(3).toString must startWith("MULTIPOINT")
        row(4).toString must startWith("MULTILINE")
        row(5).toString must startWith("MULTIPOLYGON")
      } and checkSql("SELECT ST_Buffer(pt, 1), ST_Buffer(polys, 1), ST_Buffer(geoms, 1) FROM TEST_GEOM_DATA LIMIT 1", PrepareTestDataStore.model) {
        resultArray => resultArray must haveLength(1)
      }
    }

    "correctly handle null values" in {
      checkSql("SELECT * FROM TEST_NULL_DATA LIMIT 1", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(1)
        val row = resultArray.head
        row must haveLength(10)
        Result.foreach(Range(1, 10)) { k =>
          row(k) must beNull
        }
      }
    }
  }
}
