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
 * Testing predicate/filter push down
 */
@RunWith(classOf[JUnitRunner])
class FilterTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds

  "Push down equivalence filter" should {
    "push down ID equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE __FID__ = '#23'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*#23""".r))
    }
    "push down integer attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE I = 26", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*26""".r))
    }
    "push down long attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE L = 10000000026", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*10000000026""".r))
    }
    "push down long attr equivalence 2" in {
      verify("SELECT * FROM TEST_DATA WHERE L = 26", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*26""".r), notEmpty = false)
    }
    "push down float attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE F = CAST(2.6 AS REAL)", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2\.6""".r))
    }
    "push down float attr equivalence 2" in {
      verify("SELECT * FROM TEST_DATA WHERE F = 2.6", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2\.6""".r))
    }
    "push down float attr equivalence 3" in {
      verify("SELECT * FROM TEST_DATA WHERE F = CAST(2.6 AS DECIMAL)", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2\.6""".r))
    }
    "push down double attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE D = 0.26", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*0\.26""".r))
    }
    "push down double attr equivalence 2" in {
      verify("SELECT * FROM TEST_DATA WHERE D = CAST(0.26 AS REAL)", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*0\.26""".r))
    }
    "push down double attr equivalence 3" in {
      verify("SELECT * FROM TEST_DATA WHERE D = CAST(0.26 AS DECIMAL)", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*0\.26""".r))
    }
    "push down float attr equivalence with integer" in {
      verify("SELECT * FROM TEST_DATA WHERE F = 9", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*9""".r))
    }
    "push down double attr equivalence with integer" in {
      verify("SELECT * FROM TEST_DATA WHERE D = 9", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*9""".r))
    }
    "push down string attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE S = 'str0022'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*str0022""".r))
    }
    "push down uuid attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE U = '00000000-0000-0000-0000-000000000995'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*00995""".r))
    }
    "push down timestamp attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE TS = TIMESTAMP '2022-05-01 00:15:55'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2022""".r))
    }
    "push down timestamp attr equivalence" in {
      verify("SELECT * FROM TEST_DATA WHERE TS = '2022-05-01 00:15:55'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2022""".r))
    }
  }

  "Push down comparison filter" should {
    "cannot push down ID comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE __FID__ > '#978'", Seq("""GeoMesaPhysicalTableScan(?!.*gtFilter)""".r))
    }
    "push down integer comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE I > 500", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE I >= 500", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE I < 500", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE I <= 500", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500""".r))
    }
    "push down long comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE L > 10000000955", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*10000000955""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE L >= 10000000955", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*10000000955""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE L < 10000000955", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*10000000955""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE L <= 10000000955", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*10000000955""".r))
    }
    "push down long comparison 2" in {
      verify("SELECT * FROM TEST_DATA WHERE L > 955", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*955""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE L >= 955", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*955""".r))
    }
    "push down float comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE F > 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE F >= 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE F < 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE F <= 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r))
    }
    "push down double comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE D > 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE D >= 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE D < 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE D <= 5.1", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*5\.1""".r))
    }
    "push down string comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE S > 'str0895'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*str0895""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE S >= 'str0895'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*str0895""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE S < 'str0895'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*str0895""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE S <= 'str0895'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*str0895""".r))
    }
    "push down timestamp comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE TS > TIMESTAMP '2022-05-01 00:15:55'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2022""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE TS >= '2022-05-01 00:15:55'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2022""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE TS < '2022-05-01 00:15:55'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2022""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE TS <= TIMESTAMP '2022-05-01 00:15:55'", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*2022""".r))
    }
  }

  "Push down range comparison filter" should {
    "push down integer range comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE I > 500 AND I < 600", Seq.empty, Seq("""EnumerableCalc""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE I >= 500 AND I <= 600", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down float/double range comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE F > '2.5' AND F < '8.6'", Seq.empty, Seq("""EnumerableCalc""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE F >= '2.5' AND F <= '8.6'", Seq.empty, Seq("""EnumerableCalc""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE D > '2.5' AND D < '8.6'", Seq.empty, Seq("""EnumerableCalc""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE D >= '2.5' AND D <= '8.6'", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down str range comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE S > 'str0300' AND S < 'str0500'", Seq.empty, Seq("""EnumerableCalc""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE S >= 'str0300' AND S <= 'str0500'", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down date range comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE TS > '2022-05-01 00:15:55' AND TS < '2022-05-01 00:20:00'", Seq.empty, Seq("""EnumerableCalc""".r)) and
        verify("SELECT * FROM TEST_DATA WHERE TS >= TIMESTAMP '2022-05-01 00:15:55' AND TS <= TIMESTAMP '2022-05-01 00:20:00'", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down complex integer range comparison" in {
      verify("SELECT * FROM TEST_DATA WHERE (I > 500 AND I < 600) OR (I >= 50 AND I <= 60)", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down complex integer range comparison with half range 1" in {
      verify("SELECT * FROM TEST_DATA WHERE (I > 500 AND I < 600) OR (I <= 60)", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down complex integer range comparison with half range 2" in {
      verify("SELECT * FROM TEST_DATA WHERE (I > 500 AND I < 600) OR (I >= 800)", Seq.empty, Seq("""EnumerableCalc""".r))
    }
  }

  "Push down in filter" should {
    "push down ID IN filter" in {
      verify("SELECT * FROM TEST_DATA WHERE __FID__ IN ('#38', '#138', '#238')", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down attr IN filter" in {
      verify("SELECT * FROM TEST_DATA WHERE I IN ('38', '138', '238')", Seq.empty, Seq("""EnumerableCalc""".r))
    }
  }

  "Push down IS NULL filter" should {
    "push down IS NULL filter" in {
      verify("SELECT * FROM TEST_DATA WHERE I IS NULL", Seq.empty, Seq("""EnumerableCalc""".r), notEmpty = false)
    }
    "push down IS NULL filter 2" in {
      verifyGeometry("SELECT * FROM TEST_NULL_DATA WHERE I IS NULL", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down IS NOT NULL filter" in {
      verify("SELECT * FROM TEST_DATA WHERE I IS NOT NULL", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down IS NOT NULL filter 2" in {
      verifyGeometry("SELECT * FROM TEST_NULL_DATA WHERE I IS NOT NULL", Seq.empty, Seq("""EnumerableCalc""".r), notEmpty = false)
    }
  }

  "Push down like filter" should {
    "push down like filter" in {
      verify("SELECT * FROM TEST_DATA WHERE S LIKE 'str_00_'", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down like filter 2" in {
      verify("SELECT * FROM TEST_DATA WHERE S LIKE 'str%00%'", Seq.empty, Seq("""EnumerableCalc""".r))
    }
  }

  "Push down filter with geometry condition" should {
    "push down bbox" in {
      verifyGeometry("SELECT * FROM TEST_GEOM_DATA WHERE ST_Contains(ST_MakeEnvelope(3, 3, 8, 8), pt)",
        Seq.empty,
        Seq("""EnumerableCalc""".r))
    }

    "push down other predicates" in {
      val predicates = Seq(
        "ST_Contains(ST_MakeEnvelope(3, 3, 8, 8), pt)",
        "ST_Crosses(ST_MakeEnvelope(3, 3, 8, 8), line)",
        "ST_Overlaps(ST_MakeEnvelope(10, 10, 80, 80), poly)",
        "ST_DWithin(ST_MakeEnvelope(3, 3, 8, 8), pt, 5)"
      )
      Result.foreach(predicates) { predicate =>
        verifyGeometry(s"SELECT * FROM TEST_GEOM_DATA WHERE $predicate",
          Seq.empty,
          Seq("""EnumerableCalc""".r))
      }
    }
  }

  "Push down filter with compound conditions" should {
    "push down A and B" in {
      verify("SELECT * FROM TEST_DATA WHERE I > 500 AND F < 60", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down A or B" in {
      verify("SELECT * FROM TEST_DATA WHERE I > 500 OR F < 10", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down geomA and geomB" in {
      verifyGeometry("SELECT * FROM TEST_GEOM_DATA WHERE ST_Contains(ST_MakeEnvelope(3, 3, 8, 8), pt) AND ST_Within(pt, ST_MakeEnvelope(2, 3, 8, 7))",
        Seq.empty,
        Seq("""EnumerableCalc""".r))
    }
  }

  "Push down negated conditions" should {
    "push down not A" in {
      verify("SELECT * FROM TEST_DATA WHERE NOT I > 500", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down not equal" in {
      verify("SELECT * FROM TEST_DATA WHERE I <> 500", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    // TODO: This test won't pass due to a test in GeoMesa. We'll fix it someday.
    // "push down not in" in {
    //   verify("SELECT * FROM TEST_DATA WHERE I NOT IN (500, 600)", Seq.empty, Seq("""EnumerableCalc""".r))
    // }
    "push down not contains" in {
      verifyGeometry("SELECT * FROM TEST_GEOM_DATA WHERE NOT ST_Contains(ST_MakeEnvelope(3, 3, 8, 8), pt)",
        Seq.empty,
        Seq("""EnumerableCalc""".r))
    }
  }

  "Preserve filters which cannot be pushed down" should {
    "preserve filter" in {
      verify("SELECT * FROM TEST_DATA WHERE COS(F) > 0", Seq(
        """GeoMesaPhysicalTableScan""".r,
        """EnumerableCalc""".r
      ))
    }
  }

  "Push down partial filter" should {
    "push down partial" in {
      verify("SELECT * FROM TEST_DATA WHERE COS(F) > 0 AND I > 500", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter=.*500""".r,
        """EnumerableCalc""".r
      ))
    }
  }

  "Push down filters when using with expression" should {
    "push down filter when using with clause" in {
      verify("WITH q1 AS (SELECT * FROM TEST_DATA) SELECT * FROM q1 WHERE I > 500", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down partial filter when using with clause" in {
      verify("WITH q1 AS (SELECT * FROM TEST_DATA) SELECT * FROM q1 WHERE COS(F) > 0 AND I > 500", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter=.*500""".r,
        """EnumerableCalc""".r
      ))
    }
  }

  private def verify(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and
      verifyResult(sql, PrepareTestDataStore.model, "model-csv.yaml", notEmpty = notEmpty)
  }

  private def verifyGeometry(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and
      verifyResult(sql, PrepareTestDataStore.model, PrepareTestDataStore.scannableModel, notEmpty = notEmpty)
  }
}
