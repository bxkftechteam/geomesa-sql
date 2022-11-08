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
import com.spatialx.geomesa.sql.GeoMesaCalciteTestUtils.{checkSql, verifyPlan, verifyResult}
import org.specs2.execute.Result
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.matching.Regex

/**
 * Test limit clause push down
 */
@RunWith(classOf[JUnitRunner])
class LimitTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds

  "Limit push down" should {
    "Push down limit to table scan" in {
      verify("SELECT D, S FROM TEST_DATA WHERE I > 500 AND F < 60 LIMIT 1000", Seq.empty,
        Seq("EnumerableCalc".r, "EnumerableLimit".r))
    }
    "Push down offset should work correctly" in {
      checkSql("SELECT D, S FROM TEST_DATA WHERE I > 500 AND F < 60 OFFSET 3", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(96)
      }
    }
    "Pushed down limit should work correctly" in {
      checkSql("SELECT D, S FROM TEST_DATA WHERE I > 500 LIMIT 10", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(10)
      }
    }
    "Pushed down offset-fetch should work correctly" in {
      checkSql("SELECT D, S FROM TEST_DATA WHERE I > 500 OFFSET 3 FETCH NEXT 5 ROWS ONLY", PrepareTestDataStore.model) { resultArray =>
        resultArray must haveLength(5)
      }
    }
    "Don't push down limit when result is ordered" in {
      verify("SELECT D, S FROM TEST_DATA WHERE I > 500 AND F < 60 ORDER BY I LIMIT 10",
        Seq("GeoMesaPhysicalTableScan".r, "EnumerableLimit".r),
        Seq("EnumerableCalc".r))
    }
    "Pushed down offset-fetch on aggregated table scan should work correctly" in {
      val sql = "SELECT S, COUNT(*) FROM TEST_DATA WHERE I > 900 GROUP BY S OFFSET 3 FETCH NEXT 5 ROWS ONLY"
      verifyPlan(sql, PrepareTestDataStore.model,
        Seq("""GeoMesaPhysicalTableScan.*stats=.*offset=.*""".r),
        Seq("EnumerableAggregate".r, "EnumerableLimit".r, "EnumerableCalc".r)) and
        checkSql(sql, PrepareTestDataStore.model) { resultArray =>
          resultArray must haveLength(5)
        }
    }
    "Don't push down limit to table scan when enumerable calc is needed" in {
      verify("SELECT D, S FROM TEST_DATA WHERE I > 500 AND COS(F) > 0 LIMIT 1000", Seq(
        """GeoMesaPhysicalTableScan.*gtFilter=.*500.*project=.*[FDS]+""".r,
        """EnumerableLimit""".r))
    }
  }

  private def verify(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and verifyResult(sql, PrepareTestDataStore.model, "model-csv.yaml", notEmpty = notEmpty)
  }
}
