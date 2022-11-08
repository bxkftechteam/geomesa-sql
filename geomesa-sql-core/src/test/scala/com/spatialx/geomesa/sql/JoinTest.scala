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
 * Test index lookup join
 */
@RunWith(classOf[JUnitRunner])
class JoinTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds

  "Join optimization rule" should {
    "use index lookup join on feature Id" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.__FID__ = b.__FID__", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }

    "use index lookup join on indexed int attr" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.I = b.I", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }
    "use index lookup join on indexed long attr" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.L = b.L", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }
    "use index lookup join on indexed boolean attr" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.L = b.L WHERE a.I < 10 AND b.I < 10", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }
    "use index lookup join on indexed date attr" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.TS = b.TS", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }

    "join on uuid attr should work correctly" in {
      // UUID attribute cannot be indexed, so it cannot be converted to a GeoMesaIndexLookupJoin
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.U = b.U", Seq.empty)
    }

    "don't use index lookup join on non-indexed attr" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.F = b.F", Seq("Enumerable.*Join".r), Seq("GeoMesaIndexLookupJoin".r))
    }

    "work correctly with non trivial equiv conditions" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.D * 100 = b.F * 10", Seq.empty)
    }

    "use index lookup join on multiple indexed equi condition" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.I = b.I AND a.L = b.L", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }
    "use index lookup join on partially indexed equi condition" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.I = b.I AND a.F = b.F", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }
    "use index lookup join on partially indexed equi condition 2" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.I = b.I AND a.F >= b.F", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }

    "left join should work correctly" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN TEST_DATA b ON a.I = b.I AND b.B", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }
    "inner join should work correctly" in {
      verify("SELECT * FROM TEST_DATA a JOIN TEST_DATA b ON a.I = b.I AND b.B", Seq("GeoMesaIndexLookupJoin".r), Seq("Enumerable.*Join".r))
    }

    "left join should work correctly 2" in {
      verify("SELECT * FROM TEST_DATA a LEFT JOIN (SELECT * FROM TEST_DATA WHERE F < 10) b ON a.I = b.I", Seq(
        "GeoMesaIndexLookupJoin".r,
        """GeoMesaPhysicalTableScan.*gtFilter=.*10""".r), Seq("Enumerable.*Join".r))
    }
    "inner join should work correctly 2" in {
      verify("SELECT * FROM TEST_DATA a JOIN (SELECT * FROM TEST_DATA WHERE F < 10) b ON a.I = b.I", Seq(
        "GeoMesaIndexLookupJoin".r,
        """GeoMesaPhysicalTableScan.*gtFilter.*10""".r), Seq("Enumerable.*Join".r))
    }

    "should work with projection and predicate push down correctly" in {
      verify("SELECT a.__FID__, a.I, a.F, b.D FROM TEST_DATA a JOIN (SELECT * FROM TEST_DATA WHERE F < 100) b ON a.I = b.I WHERE a.D > 0.1 AND COS(a.F) > 0", Seq(
        "GeoMesaIndexLookupJoin".r,
        """GeoMesaPhysicalTableScan.*gtFilter.*0\.1.*project""".r,
        """GeoMesaPhysicalTableScan.*gtFilter.*100.*project""".r), Seq("Enumerable.*Join".r))
    }
    "should work with projection and predicate push down correctly 2" in {
      verify("SELECT a.__FID__, a.I, a.F, b.D FROM TEST_DATA a JOIN (SELECT * FROM TEST_DATA WHERE F < 100 AND COS(F) > 0) b ON a.I = b.I WHERE a.D > 0.1 AND COS(a.F) > 0", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter.*0\.1.*project""".r,
        """GeoMesaPhysicalTableScan.*gtFilter.*100.*project""".r))
    }

    "should work with projection, filter, sort and limit" in {
      verify("SELECT a.__FID__, a.I, a.F, b.D FROM TEST_DATA a JOIN (SELECT * FROM TEST_DATA WHERE F < 100) b ON a.I = b.I WHERE a.D > 0.1 AND COS(a.F) > 0 ORDER BY a.F LIMIT 10", Seq(
        "GeoMesaIndexLookupJoin".r,
        """GeoMesaPhysicalTableScan.*gtFilter.*0\.1.*project""".r,
        """GeoMesaPhysicalTableScan.*gtFilter.*100.*project""".r), Seq("Enumerable.*Join".r)) and
        verify("SELECT a.__FID__, a.I, a.F, b.D FROM TEST_DATA a JOIN (SELECT * FROM TEST_DATA WHERE F < 100 AND COS(F) > 0) b ON a.I = b.I WHERE a.D > 0.1 AND COS(a.F) > 0 ORDER BY a.I LIMIT 10", Seq(
          """GeoMesaPhysicalTableScan.*gtFilter.*0\.1.*project""".r,
          """GeoMesaPhysicalTableScan.*gtFilter.*100.*project""".r))
    }

    "should work with join operands swapping optimization" in {
      // Right hand side is a subquery with order and limit. Swapping it to the left side would reduce the number of lookups to the table
      verify("SELECT * FROM TEST_DATA a JOIN (SELECT * FROM TEST_DATA WHERE F < 10 ORDER BY F LIMIT 10) b ON a.I = b.I", Seq(
        "GeoMesaIndexLookupJoin".r,
        """GeoMesaPhysicalTableScan.*gtFilter.*F < 10""".r)) and
        verify("SELECT * FROM TEST_DATA a JOIN (SELECT * FROM TEST_DATA WHERE F < 10 ORDER BY F LIMIT 10) b ON a.I = b.I WHERE a.F < 0.6", Seq(
          "GeoMesaIndexLookupJoin".r,
          """GeoMesaPhysicalTableScan.*gtFilter.*F < 10""".r,
          """GeoMesaPhysicalTableScan.*gtFilter.*F < 0\.6""".r))
    }
  }

  private def verify(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and verifyResult(sql, PrepareTestDataStore.model, "model-csv.yaml", notEmpty = notEmpty)
  }
}
