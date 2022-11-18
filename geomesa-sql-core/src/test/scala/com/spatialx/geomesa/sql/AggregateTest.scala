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
import com.spatialx.geomesa.sql.GeoMesaCalciteTestUtils.{verifyPlan, verifyResult}
import org.specs2.execute.Result
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.matching.Regex

/**
 * Test aggregation push down
 */
@RunWith(classOf[JUnitRunner])
class AggregateTest extends Specification {

  val ds: DataStore = PrepareTestDataStore.ds

  "Aggregation push down" should {
    "Push down aggregate to table scan" in {
      verify("SELECT COUNT(*) FROM TEST_DATA", Seq("GeoMesaPhysicalTableScan.*stats=.*".r), Seq("EnumerableAggregate".r))
    }

    "Push down multiple aggregates to table scan" in {
      verify("SELECT COUNT(*), MIN(I), MAX(I) FROM TEST_DATA", Seq("GeoMesaPhysicalTableScan.*stats=.*".r), Seq("EnumerableAggregate".r))
    }

    "Work with various data types in non groupping mode" in {
      verify("SELECT COUNT(*), MIN(F), MAX(F), MIN(S), MAX(S), MIN(D), MAX(D), MIN(TS), MAX(TS) FROM TEST_DATA",
        Seq("GeoMesaPhysicalTableScan.*stats=.*".r), Seq("EnumerableAggregate".r))
    }

    "Cannot push down aggregations on feature ID" in {
      verify("SELECT COUNT(*), MIN(__FID__), MAX(__FID__) FROM TEST_DATA", Seq.empty, Seq("GeoMesaPhysicalTableScan.*stats=.*".r))
    }

    "Push down aggregate with group(string) to table scan" in {
      verify("SELECT S, COUNT(*) FROM TEST_DATA GROUP BY S",
        Seq("GeoMesaPhysicalTableScan.*project=.*S.*stats=.*".r),
        Seq("EnumerableAggregate".r, "EnumerableCalc".r))
    }

    "Push down aggregate with group(int) to table scan" in {
      verify("SELECT I, COUNT(*) FROM TEST_DATA GROUP BY I",
        Seq("GeoMesaPhysicalTableScan.*project=.*I.*stats=.*".r),
        Seq("EnumerableAggregate".r, "EnumerableCalc".r))
    }

    "Push down aggregate with group(long) to table scan" in {
      verify("SELECT L, COUNT(*) FROM TEST_DATA GROUP BY L",
        Seq("GeoMesaPhysicalTableScan.*project=.*L.*stats=.*".r),
        Seq("EnumerableAggregate".r))
    }

    "Push down aggregate with group(float) to table scan" in {
      verify("SELECT F, COUNT(*) FROM TEST_DATA GROUP BY F",
        Seq("GeoMesaPhysicalTableScan.*project=.*F.*stats=.*".r),
        Seq("EnumerableAggregate".r))
    }

    "Push down aggregate with group(double) to table scan" in {
      verify("SELECT D, COUNT(*) FROM TEST_DATA GROUP BY D",
        Seq("GeoMesaPhysicalTableScan.*project=.*D.*stats=.*".r),
        Seq("EnumerableAggregate".r))
    }

    "Push down aggregate with group(timestamp) to table scan" in {
      verify("SELECT TS, COUNT(*) FROM TEST_DATA GROUP BY TS",
        Seq("GeoMesaPhysicalTableScan.*project=.*TS.*stats=.*".r),
        Seq("EnumerableAggregate".r))
    }

    "Aggregate with group(uuid) to table scan" in {
      verify("SELECT U, COUNT(*) FROM TEST_DATA GROUP BY U", Seq("""GeoMesaPhysicalTableScan.*project=.*U""".r))
    }

    "Aggregate with group(boolean) to table scan" in {
      verify("SELECT B, COUNT(*) FROM TEST_DATA GROUP BY B", Seq("""GeoMesaPhysicalTableScan.*project=.*B""".r))
    }

    "Push down aggregate with group to table scan 2" in {
      verify("SELECT COUNT(*) FROM TEST_DATA GROUP BY S", Seq("GeoMesaPhysicalTableScan.*stats=.*".r),
        Seq("EnumerableAggregate".r))
    }

    "Push down various aggregations" in {
      val aggs = Seq("MIN(I)", "MAX(L)", "MIN(F)", "MAX(D)", "MIN(S)", "MIN(TS)")
      Result.foreach(aggs){ agg =>
        verify(s"SELECT S, $agg FROM TEST_DATA GROUP BY S",
          Seq("GeoMesaPhysicalTableScan.*project=.*S.*stats=.*".r),
          Seq("EnumerableAggregate".r))
      }
    }

    "Push down multiple aggregates with group to table scan" in {
      verify("SELECT S, COUNT(*), MIN(I), MAX(I), MAX(L), MAX(F), MIN(D), MAX(S), MIN(S), MIN(TS) FROM TEST_DATA GROUP BY S",
        Seq("GeoMesaPhysicalTableScan.*project=.*S.*stats=.*".r),
        Seq("EnumerableAggregate".r))
    }

    "Aggregates unsupported attrs with group to table scan" in {
      val aggs = Seq("MIN(B)", "MAX(U)", "MIN(TS)")
      Result.foreach(aggs){ agg =>
        verify(s"SELECT S, $agg FROM TEST_DATA GROUP BY S", Seq("""GeoMesaPhysicalTableScan.*project=.*S""".r))
      } and verify("SELECT S, COUNT(*), MIN(B), MAX(U), MIN(TS) FROM TEST_DATA GROUP BY S", Seq(
        """GeoMesaPhysicalTableScan.*project=.*S""".r))
    }

    "Cannot push down aggregations grouping by multiple columns" in {
      verify("SELECT S, I, COUNT(*) FROM TEST_DATA GROUP BY S, I", Seq("""GeoMesaPhysicalTableScan.*project=.*S""".r))
    }

    "Cannot push down aggregations grouping by feature ID" in {
      verify("SELECT __FID__, COUNT(*) FROM TEST_DATA GROUP BY __FID__", Seq("""GeoMesaPhysicalTableScan.*project=.*__FID__""".r))
    }

    "Push down aggregate and filter to table scan" in {
      verify("SELECT S, COUNT(*) FROM TEST_DATA WHERE I > 900 GROUP BY S LIMIT 100", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter=.*900.*project=.*S.*stats=.*""".r),
        Seq("EnumerableAggregate".r, "EnumerableCalc".r))
    }

    "Cannot push down aggregate if filter cannot be fully pushed down" in {
      verify("SELECT S, COUNT(*) FROM TEST_DATA WHERE I > 900 AND COS(F) > 0 GROUP BY S", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter=.*900.*project=.*S""".r))
    }

    "Push outer predicate into table scan" in {
      verify("WITH q1 AS (SELECT F, COUNT(1) AS C, MIN(I) AS MINI, MAX(S) AS MAXS FROM TEST_DATA WHERE F < 10 GROUP BY F) SELECT F, C, MINI FROM q1 WHERE F > 8",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*F > 8.*stats=""".r), Seq("EnumerableCalc".r, "EnumerableAggregate".r))
    }

    "Don't push outer predicate using aggregated values into table scan" in {
      verify("WITH q1 AS (SELECT F, COUNT(1) AS C, MIN(I) AS MINI, MAX(S) AS MAXS FROM TEST_DATA WHERE F < 10 GROUP BY F) SELECT F, C, MINI FROM q1 WHERE MINI > 80",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*F < 10.*stats=""".r))
    }

    "Handle group by boolean without aggregation function" in {
      verify("SELECT B FROM TEST_DATA WHERE I < 50 GROUP BY B",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*I < 50.*project=.*B.*""".r))
    }

    "Handles nested aggregation on boolean column correctly" in {
      verify("SELECT COUNT(*) FROM (SELECT B, COUNT(*) FROM TEST_DATA WHERE I > 900 GROUP BY B)", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter=.*900.*project=.*B""".r))
    }

    "Push down group by integer without aggregation function" in {
      verifyScannable("SELECT I FROM TEST_REPEATED_DATA WHERE I < 5 GROUP BY I",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5.*stats=.*Enumeration""".r),
        Seq("EnumerableCalc".r, "EnumerableAggregate".r)) and
        verifyScannable("SELECT I FROM TEST_REPEATED_DATA WHERE I < 1 GROUP BY I",
          Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*1.*stats=.*Enumeration""".r),
          Seq("EnumerableCalc".r, "EnumerableAggregate".r)) and
        verifyScannable("SELECT I FROM TEST_REPEATED_DATA WHERE I < 0 GROUP BY I",
          Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*0.*stats=.*Enumeration""".r),
          Seq("EnumerableCalc".r, "EnumerableAggregate".r), notEmpty = false)
    }

    "Push down group by string without aggregation function" in {
      verifyScannable("SELECT S FROM TEST_REPEATED_DATA WHERE I < 5 GROUP BY S",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5.*stats=.*Enumeration""".r),
        Seq("EnumerableCalc".r, "EnumerableAggregate".r))
    }

    "Push down group by float without aggregation function" in {
      verifyScannable("SELECT F FROM TEST_REPEATED_DATA WHERE I < 5 GROUP BY F",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5.*stats=.*Enumeration""".r),
        Seq("EnumerableCalc".r, "EnumerableAggregate".r))
    }

    "Push down group by date without aggregation function" in {
      verifyScannable("SELECT TS FROM TEST_REPEATED_DATA WHERE I < 5 GROUP BY TS",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5.*stats=.*Enumeration""".r),
        Seq("EnumerableCalc".r, "EnumerableAggregate".r))
    }

    "Handles nested aggregation on string column correctly" in {
      verifyScannable("SELECT COUNT(*) FROM (SELECT S, COUNT(*) FROM TEST_REPEATED_DATA WHERE I < 5 GROUP BY S)", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter=.*5""".r))
    }

    "Push down SELECT DISTINCT" in {
      verifyScannable("SELECT DISTINCT S FROM TEST_REPEATED_DATA WHERE I < 5",
        Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5.*stats=.*Enumeration""".r),
        Seq("EnumerableCalc".r, "EnumerableAggregate".r))
    }

    "Handles SELECT DISTINCT with multiple columns correctly" in {
      verifyScannable("SELECT DISTINCT I, S FROM TEST_REPEATED_DATA WHERE I < 5", Seq.empty) and
        verifyScannable("SELECT DISTINCT * FROM TEST_REPEATED_DATA WHERE I < 5", Seq.empty)
    }
  }

  private def verify(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and verifyResult(sql, PrepareTestDataStore.model, "model-csv.yaml", notEmpty = notEmpty)
  }

  private def verifyScannable(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and verifyResult(sql, PrepareTestDataStore.model, PrepareTestDataStore.scannableModel, notEmpty = notEmpty)
  }
}
