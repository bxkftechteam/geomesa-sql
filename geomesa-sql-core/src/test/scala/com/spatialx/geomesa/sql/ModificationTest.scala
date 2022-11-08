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
import com.spatialx.geomesa.sql.GeoMesaCalciteTestUtils.{checkSql, compareResults, runSql, runUpdate}
import com.spatialx.geomesa.sql.PrepareTestDataStore.prepareTestDataForModification
import org.specs2.execute.Result
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.sql.Timestamp
import java.util.Date

/**
 * Test for data modifications (INSERT, UPDATE, DELETE)
 */
@RunWith(classOf[JUnitRunner])
class ModificationTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds
  val ts: Long = System.currentTimeMillis()

  "TableModify INSERT" should {
    "should work with all data types" in {
      prepareTestDataForModification(ds, "insert_into_test_data", 0)
      val rows = runUpdate(
        """INSERT INTO insert_into_test_data VALUES
          |('#1', 1, 10, 0.1, 0.01,'00000000-0000-0000-0000-000000000001', 'str001', true, '2022-05-01 12:10:23',
          |x'45F0AB', ST_GeomFromText('POINT (1 1)'))""".stripMargin, PrepareTestDataStore.model)
      rows mustEqual 1L and checkSql("SELECT *, ST_AsText(geom) FROM insert_into_test_data", PrepareTestDataStore.model) { rows =>
        rows should haveLength(1)
        val row = rows.head
        row(0) mustEqual "#1"
        row(1) mustEqual 1
        row(2) mustEqual 10
        row(3) mustEqual 0.1f
        row(4) mustEqual 0.01
        row(5) mustEqual "00000000-0000-0000-0000-000000000001"
        row(6) mustEqual "str001"
        row(7) mustEqual true
        row(8) must beAnInstanceOf[Date]
        row(8).asInstanceOf[Date].getTime mustEqual 1651407023000L
        row(9) must beAnInstanceOf[Array[Byte]]
        row(9) mustEqual Array[Byte](0x45.toByte, 0xF0.toByte, 0xAB.toByte)
        row(11) mustEqual "POINT (1 1)"
      }
    }

    "should insert null values" in {
      prepareTestDataForModification(ds, "insert_into_test_data_null_values", 0)
      val rows = runUpdate(
        """INSERT INTO insert_into_test_data_null_values (I, L, F, D, U, S, TS, bytes, geom) VALUES
          |(null, 10, 0.1, null, null, 'str001', null, null, ST_GeomFromText('POINT (1 1)'))""".stripMargin, PrepareTestDataStore.model)
      rows mustEqual 1L and checkSql("SELECT *, ST_AsText(geom) FROM insert_into_test_data_null_values", PrepareTestDataStore.model) { rows =>
        rows should haveLength(1)
        val row = rows.head
        row(0).asInstanceOf[String] must not(beEmpty)
        row(1) must beNull
        row(2) mustEqual 10
        row(3) mustEqual 0.1f
        row(4) must beNull
        row(5) must beNull
        row(6) mustEqual "str001"
        row(7) must beNull
        row(8) must beNull
        row(9) must beNull
        row(11) mustEqual "POINT (1 1)"
      }
    }

    "should insert partial fields" in {
      prepareTestDataForModification(ds, "insert_into_test_data_partial", 0)
      val rows = runUpdate(
        """INSERT INTO insert_into_test_data_partial (L, D, geom) VALUES
          |(10, 0.1, ST_GeomFromText('POINT (1 1)'))""".stripMargin, PrepareTestDataStore.model)
      rows mustEqual 1L and checkSql("SELECT *, ST_AsText(geom) FROM insert_into_test_data_partial", PrepareTestDataStore.model) { rows =>
        rows should haveLength(1)
        val row = rows.head
        row(0).asInstanceOf[String] must not(beEmpty)
        row(1) must beNull
        row(2) mustEqual 10
        row(3) must beNull
        row(4) mustEqual 0.1
        row(5) must beNull
        row(6) must beNull
        row(7) must beNull
        row(8) must beNull
        row(9) must beNull
        row(11) mustEqual "POINT (1 1)"
        ok
      }
    }

    "should insert values from another table" in {
      prepareTestDataForModification(ds, "insert_into_test_data_from_table", 1000)
      prepareTestDataForModification(ds, "insert_into_test_data_to_table", 0)
      val rows = runUpdate(
        "INSERT INTO insert_into_test_data_to_table SELECT * FROM insert_into_test_data_from_table",
        PrepareTestDataStore.model)
      val original = runSql("SELECT * FROM insert_into_test_data_from_table", PrepareTestDataStore.model)
      val inserted = runSql("SELECT * FROM insert_into_test_data_to_table", PrepareTestDataStore.model)
      rows mustEqual 1000L and compareResults(original, inserted, true)
    }

    "should insert values from subquery" in {
      prepareTestDataForModification(ds, "insert_into_test_data_from_table_2", 1000)
      prepareTestDataForModification(ds, "insert_into_test_data_to_table_2", 0)
      val rows = runUpdate(
        """INSERT INTO insert_into_test_data_to_table_2 (__FID__, I, D, TS, geom)
          |SELECT __FID__, I * 2, D + 100, TS, ST_MakePoint(ST_Y(geom), ST_X(geom)) FROM insert_into_test_data_from_table_2""".stripMargin,
        PrepareTestDataStore.model)
      val original = runSql("SELECT I * 2, NULL, NULL, D + 100, NULL, NULL, NULL, TS, NULL, ST_MakePoint(ST_Y(geom), ST_X(geom))" +
        " FROM insert_into_test_data_from_table_2", PrepareTestDataStore.model)
      val inserted = runSql("SELECT I, L, F, D, U, S, B, TS, bytes, geom FROM insert_into_test_data_to_table_2", PrepareTestDataStore.model)
      rows mustEqual 1000L and compareResults(original, inserted, true)
    }

    "should insert values from empty subquery" in {
      prepareTestDataForModification(ds, "insert_into_test_data_from_table_3", 10)
      prepareTestDataForModification(ds, "insert_into_test_data_to_table_3", 0)
      val rows = runUpdate(
        "INSERT INTO insert_into_test_data_to_table_3 SELECT * FROM insert_into_test_data_from_table_3 WHERE I > 100",
        PrepareTestDataStore.model)
      rows mustEqual 0 and checkSql("SELECT * FROM insert_into_test_data_to_table_3", PrepareTestDataStore.model) { rows =>
        rows should beEmpty
      }
    }
  }

  "TableModify UPDATE and DELETE" should {
    "should update and delete" in {
      val tableName = s"update_test_data_$ts"
      prepareTestDataForModification(ds, tableName, 10)
      val updatedRows = runUpdate(
        s"""UPDATE $tableName SET I = I + 1000, D = D * 10, TS = CURRENT_TIMESTAMP,
             |geom = ST_MakePoint(10, 10) WHERE MOD(I, 2) = 0""".stripMargin, PrepareTestDataStore.model)
      val updateCheckResult = checkSql(s"SELECT I, F, D, TS, ST_AsText(geom) FROM $tableName", PrepareTestDataStore.model) { rows =>
        Result.foreach(rows) { row =>
          val i = row(0).asInstanceOf[Int]
          val f = row(1).asInstanceOf[Float]
          val d = row(2).asInstanceOf[Double]
          val t = row(3).asInstanceOf[Timestamp].getTime
          val wkt = row(4).asInstanceOf[String]
          if (i < 1000) {
            d * 10 must beCloseTo(f, 1E-4)
          } else {
            (d must beCloseTo(f, 1E-4)) and
            (t must beGreaterThanOrEqualTo(ts)) and
            (wkt mustEqual "POINT (10 10)")
          }
        }
      }
      val deletedRows = runUpdate(s"DELETE FROM $tableName WHERE MOD(I, 2) <> 0", PrepareTestDataStore.model)
      val deleteCheckResult = checkSql(s"SELECT I FROM $tableName", PrepareTestDataStore.model) { rows =>
        rows must haveLength(5) and Result.foreach(rows) { row =>
          val i = row(0).asInstanceOf[Int]
          i must beGreaterThanOrEqualTo(1000)
        }
      }
      updatedRows mustEqual 5 and (deletedRows mustEqual 5) and updateCheckResult and deleteCheckResult
    }
  }
}
