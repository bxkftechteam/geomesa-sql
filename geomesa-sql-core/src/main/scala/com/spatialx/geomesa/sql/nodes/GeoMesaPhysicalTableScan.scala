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
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.geotools.filter.text.ecql.ECQL
import com.spatialx.geomesa.sql.nodes.GeoMesaTableScan.ScanParams
import com.spatialx.geomesa.sql.rules.GeoMesaRules
import com.spatialx.geomesa.sql.{GeoMesaQueryParams, GeoMesaTranslatableTable}

import java.util

/**
 * A physical version of GeoMesaTableScan node, will be converted from GeoMesaLogicalTableScan by
 * converter rule GeoMesaTableLogicalToPhysicalRule.
 */
class GeoMesaPhysicalTableScan(cluster: RelOptCluster, traitSet: RelTraitSet, hints: util.List[RelHint],
                               table: RelOptTable, geomesaTable: GeoMesaTranslatableTable,
                               scanParams: ScanParams)
  extends GeoMesaTableScan(cluster, traitSet, hints, table, geomesaTable, scanParams)
    with GeoMesaRel {

  assert(getConvention == GeoMesaRel.CONVENTION)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val baseCost = super.computeSelfCost(planner, mq)
    val filterFactor = if (scanParams.gtFilter.isDefined) 0.1 else 1.0
    val columnCount = if (scanParams.columnIndexList.nonEmpty) scanParams.columnIndexList.size else table.getRowType.getFieldCount
    val pruneFactor = (columnCount + 2D) / (table.getRowType.getFieldCount + 2D)
    val aggLimitFactor = if (isAggregatedOrLimited) 0.5 else 1.0
    baseCost.multiplyBy(filterFactor * pruneFactor * aggLimitFactor)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val baseRowCount = super.estimateRowCount(mq)
    if (scanParams.gtFilter.isDefined) baseRowCount * .1 else baseRowCount
  }

  override def register(planner: RelOptPlanner): Unit = {
    GeoMesaRules.PHYSICAL_RULES.foreach(planner.addRule)
  }

  override def implement(): GeoMesaRel.Result = {
    val ecql = scanParams.gtFilter.map(ECQL.toCQL).getOrElse("")
    GeoMesaRel.Result(table, geomesaTable, queryParams = GeoMesaQueryParams(
      geomesaTable.typeName,
      ecql = ecql,
      properties = propertyNameList.toArray,
      statsStrings = scanParams.statsStrings.toArray,
      statAttributes = scanParams.statAttributes.toArray,
      offset = scanParams.offset,
      fetch = scanParams.fetch))
  }
}
