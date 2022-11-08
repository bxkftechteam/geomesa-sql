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

@RunWith(classOf[JUnitRunner])
class SpatialJoinTest extends Specification {
  val ds: DataStore = PrepareTestDataStore.ds

  "Spatial join" should {
    "Support ST_Contains" in {
      verify("SELECT * FROM test_geom_data a JOIN test_geom_data_simple b ON ST_Contains(b.poly, a.pt)", Seq("SpatialJoin".r)) and
        verify("SELECT * FROM test_geom_data_simple a JOIN test_geom_data b ON ST_Contains(a.poly, b.pt)", Seq("SpatialJoin".r))
    }
    "Support ST_Within" in {
      verify("SELECT * FROM test_geom_data a JOIN test_geom_data_simple b ON ST_Within(a.pt, b.poly)", Seq("SpatialJoin".r)) and
        verify("SELECT * FROM test_geom_data_simple a JOIN test_geom_data b ON ST_Within(b.pt, a.poly)", Seq("SpatialJoin".r))
    }
    "Support ST_Intersects" in {
      verify("SELECT * FROM test_geom_data a JOIN test_geom_data_simple b ON ST_Intersects(a.pt, b.line)", Seq("SpatialJoin".r))
    }
    "Support ST_DWithin" in {
      verify("SELECT * FROM test_geom_data a JOIN test_geom_data_simple b ON ST_DWithin(a.pt, b.line, 3)", Seq("SpatialJoin".r))
    }
    "Handle complex operands correctly" in {
      verify("SELECT * FROM test_geom_data a JOIN test_geom_data_simple b ON ST_Intersects(ST_Buffer(a.pt, 2E0), ST_Buffer(b.poly, 2E0))", Seq("SpatialJoin".r))
    }
    "Works with left join" in {
      verify("SELECT * FROM test_geom_data a LEFT JOIN test_geom_data_simple b ON ST_Contains(b.poly, a.pt)", Seq("SpatialJoin".r))
    }
    "Works with projection" in {
      verify("SELECT a.pt FROM test_geom_data a LEFT JOIN test_geom_data_simple b ON ST_Contains(b.poly, a.pt)", Seq("SpatialJoin".r))
    }
    "Don't optimize right join or full outer join" in {
      verify("SELECT a.pt FROM test_geom_data a RIGHT JOIN test_geom_data_simple b ON ST_Contains(b.poly, a.pt)", Seq.empty) and
        verify("SELECT a.pt FROM test_geom_data a FULL OUTER JOIN test_geom_data_simple b ON ST_Contains(b.poly, a.pt)", Seq.empty)
    }
    "Don't optimize spatial join when spatial predicate is complicated" in {
      verify("SELECT a.pt FROM test_geom_data a JOIN test_geom_data_simple b ON ST_Intersects(b.poly, ST_Union(a.pt, b.pt))", Seq.empty)
    }
  }

  private def verify(sql: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty, notEmpty: Boolean = true): Result = {
    verifyPlan(sql, PrepareTestDataStore.model, planPatterns, unexpectedPlanPatterns) and
      verifyResult(sql, PrepareTestDataStore.model, PrepareTestDataStore.scannableModel, notEmpty = notEmpty)
  }
}
