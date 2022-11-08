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

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import com.spatialx.geomesa.sql.nodes.GeoMesaTableScan.ScanParams
import com.spatialx.geomesa.sql.{FID_FIELD_NAME, GeoMesaTranslatableTable}
import org.opengis.filter.{Filter => GTFilter}

import java.util

/**
  * Relational expression representing a GeoMesa table. It may contain projections and filters when they were
  * pushed down to this table scan relation.
  */
abstract class GeoMesaTableScan(cluster: RelOptCluster, traitSet: RelTraitSet,
                                hints: util.List[RelHint], table: RelOptTable,
                                val geomesaTable: GeoMesaTranslatableTable,
                                val scanParams: ScanParams)
    extends TableScan(cluster, traitSet, hints, table) {

  val propertyNameList: Seq[String] = {
    val sft = geomesaTable.getSimpleFeatureType
    scanParams.columnIndexList.map { k =>
      if (k == 0) FID_FIELD_NAME else sft.getDescriptor(k - 1).getLocalName
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw)
      .itemIf("condition", scanParams. condition.orNull, scanParams.condition.isDefined)
      .itemIf("gtFilter", scanParams.gtFilter.orNull, scanParams.gtFilter.isDefined)
      .itemIf("project", propertyNameList.mkString(","), scanParams.columnIndexList.nonEmpty)
      .itemIf("stats", scanParams.statsStrings.mkString(";"), scanParams.statsStrings.nonEmpty)
      .itemIf("statAttrs", scanParams.statAttributes.mkString(";"), scanParams.statAttributes.nonEmpty)
      .itemIf("offset", scanParams.offset, scanParams.offset > 0)
      .itemIf("fetch", scanParams.fetch, scanParams.fetch > 0)

  override def deriveRowType(): RelDataType = scanParams.resultRowType.getOrElse(table.getRowType)

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    assert(inputs.isEmpty)
    this
  }

  def isAggregatedOrLimited: Boolean = scanParams.statsStrings.nonEmpty || isLimited
  def isLimited: Boolean = scanParams.offset > 0 || scanParams.fetch > 0
}

object GeoMesaTableScan {
  case class ScanParams(
    // filter
    condition: Option[RexNode] = None,
    gtFilter: Option[GTFilter] = None,
    // projection
    columnIndexList: Seq[Int] = Seq.empty,
    // derivedRowType can be affected by both projection and aggregation
    resultRowType: Option[RelDataType] = None,
    // aggregation
    statsStrings: Seq[String] = Seq.empty,
    statAttributes: Seq[String] = Seq.empty,
    // limit
    offset: Long = 0,
    fetch: Long = 0
  )
}
