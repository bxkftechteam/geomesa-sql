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
 * Testing projection push down
 */
@RunWith(classOf[JUnitRunner])
class ProjectTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds

  "Project push down" should {
    "project __FID__ to GeoMesaTableScan" in {
      verify("SELECT __FID__ FROM TEST_DATA", Seq("""GeoMesaPhysicalTableScan.*project=\[__FID__]""".r))
    }
    "project attribute to GeoMesaTableScan" in {
      verify("SELECT I,F,S,TS FROM TEST_DATA", Seq("""GeoMesaPhysicalTableScan.*project=\[I,F,S,TS]""".r))
    }
    "project feature ID and attributes to GeoMesaTableScan" in {
      verify("SELECT I,F,__FID__,TS FROM TEST_DATA", Seq("""GeoMesaPhysicalTableScan.*project=\[I,F,__FID__,TS]""".r))
    }
    "project duplicated attributes" in {
      verify("SELECT D,__FID__,__FID__,D FROM TEST_DATA", Seq("""GeoMesaPhysicalTableScan.*project=\[D,__FID__,__FID__,D]""".r))
    }
  }

  private def verify(sql: String, planPatterns: Seq[Regex]): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns) and verifyResult(sql, PrepareTestDataStore.model, "model-csv.yaml")
  }
}
