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
 * Testing projection and filter push down
 */
@RunWith(classOf[JUnitRunner])
class ProjectFilterTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds

  "Project filter push down" should {
    "Push down all projects and filters" in {
      verify("SELECT I, F, D, S FROM TEST_DATA WHERE I > 500 AND F < 60", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "Don't need to fetch filtered attrs" in {
      verify("SELECT D, S FROM TEST_DATA WHERE I > 500 AND F < 60", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "Don't need to fetch filtered feature IDs" in {
      verify("SELECT D, S FROM TEST_DATA WHERE __FID__ IN ('#100', '#200')", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "Push down partial projects and filters" in {
      verify("SELECT D, S FROM TEST_DATA WHERE I > 500 AND COS(F) > 0", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500.*project=.*[FDS]+""".r))
    }
    "Push down partial projects and filters 2" in {
      verify("SELECT D, S, F + 10 FROM TEST_DATA WHERE I > 500 AND COS(F) > 0", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500.*project=.*[FDS]+""".r))
    }
    "Push down partial projects and filters 2" in {
      verify("SELECT D, S, F + 10 FROM TEST_DATA WHERE I > 500", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500.*project=.*[FDS]+""".r))
    }
    "push down project and filter when using with clause" in {
      verify("WITH q1 AS (SELECT * FROM TEST_DATA) SELECT D, S FROM q1 WHERE I > 500", Seq.empty, Seq("""EnumerableCalc""".r))
    }
    "push down project and partial filter when using with clause" in {
      verify("WITH q1 AS (SELECT * FROM TEST_DATA) SELECT D, S FROM q1 WHERE I > 500 AND COS(F) > 0", Seq("""GeoMesaPhysicalTableScan.*gtFilter=.*500.*project=.*[FDS]+""".r))
    }
  }

  private def verify(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and verifyResult(sql, PrepareTestDataStore.model, "model-csv.yaml", notEmpty = notEmpty)
  }
}
