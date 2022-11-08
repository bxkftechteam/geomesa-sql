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

package com.spatialx.geomesa.sql.nodes

import org.apache.calcite.plan.{Convention, RelOptTable}
import org.apache.calcite.rel.RelNode
import com.spatialx.geomesa.sql.{GeoMesaQueryParams, GeoMesaTranslatableTable}

/**
  * Relational expression that uses GeoMesa calling convention.
  */
trait GeoMesaRel extends RelNode {
  /**
    * Implemented by subclasses to convert itself to geotools Query object
    */
  def implement(): GeoMesaRel.Result
}

object GeoMesaRel {
  /**
    * Calling convention for relational operations that occur in GeoMesa
    */
  val CONVENTION = new Convention.Impl("GEOMESA", classOf[GeoMesaRel])

  /**
    * Return type of implementation process that converts a tree of GeoMesaRel
    * nodes into a geotools Query object.
    */
  case class Result(
    table: RelOptTable,
    geoMesaTable: GeoMesaTranslatableTable,
    queryParams: GeoMesaQueryParams)
}
