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

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.geotools.factory.CommonFactoryFinder
import com.spatialx.geomesa.sql.GeoMesaTranslatableTable
import com.spatialx.geomesa.sql.nodes.GeoMesaLogicalTableScan.ff
import com.spatialx.geomesa.sql.nodes.GeoMesaTableScan.ScanParams
import com.spatialx.geomesa.sql.rules.GeoMesaRules
import org.opengis.filter.{FilterFactory2, Filter => GTFilter}

import java.util

/**
 * A logical version of GeoMesaTableScan node. All logical transformations such as filter/projection
 * push down were performed on GeoMesaLogicalTableScan node.
 */
class GeoMesaLogicalTableScan(cluster: RelOptCluster, traitSet: RelTraitSet, hints: util.List[RelHint],
                              table: RelOptTable, geomesaTable: GeoMesaTranslatableTable,
                              scanParams: ScanParams)
  extends GeoMesaTableScan(cluster, traitSet, hints, table, geomesaTable, scanParams) {

  import scala.collection.JavaConverters._

  assert(getConvention == Convention.NONE)

  def withFilter(rexBuilder: RexBuilder, newCondition: RexNode, newFilter: GTFilter): GeoMesaLogicalTableScan = {
    val condition = scanParams.condition
      .map(c => rexBuilder.makeCall(SqlStdOperatorTable.AND, Seq(c, newCondition).asJava))
      .orElse(Some(newCondition))
    val gtFilter = scanParams.gtFilter
      .map(f => ff.and(f, newFilter))
      .orElse(Some(newFilter))
    withScanParams(
      scanParams.copy(
        condition = condition,
        gtFilter = gtFilter))
  }

  def withProject(projectedRowType: RelDataType, newColumnIndexList: Seq[Int]): GeoMesaLogicalTableScan = {
    val columnIndexList = if (scanParams.columnIndexList.isEmpty) newColumnIndexList else {
      // Cascade new projection onto already existing projection
      newColumnIndexList.map(k => scanParams.columnIndexList(k))
    }
    withScanParams(
      scanParams.copy(
        resultRowType = Some(projectedRowType),
        columnIndexList = columnIndexList))
  }

  def withAggregate(aggResultRowType: RelDataType, statsStrings: Seq[String], statAttributes: Seq[String]): GeoMesaLogicalTableScan =
    withScanParams(
      scanParams.copy(
        resultRowType = Some(aggResultRowType),
        statsStrings = statsStrings,
        statAttributes = statAttributes))

  def withLimit(offset: Long, fetch: Long): GeoMesaLogicalTableScan =
    withScanParams(scanParams.copy(offset = offset, fetch = fetch))

  def withScanParams(scanParams: ScanParams): GeoMesaLogicalTableScan = {
    new GeoMesaLogicalTableScan(cluster, traitSet, getHints, table, geomesaTable, scanParams)
  }

  def toPhysical: GeoMesaPhysicalTableScan = {
    val physicalTraitSet = traitSet.replace(GeoMesaRel.CONVENTION)
    new GeoMesaPhysicalTableScan(cluster, physicalTraitSet, getHints, table, geomesaTable, scanParams)
  }

  override def register(planner: RelOptPlanner): Unit = {
    GeoMesaRules.COMMON_LOGICAL_RULES.foreach(planner.addRule)
    if (geomesaTable.params.getOrElse("noAggrPushdown", "false") != "true") {
      // Aggregation pushdown is painfully slow when running in GeoMesa without coprocessor or server side processing.
      // User can disable aggregation pushdown in those situations.
      planner.addRule(GeoMesaRules.AGGREGATION_RULE)
    }
  }
}

object GeoMesaLogicalTableScan {
  val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2

  def create(cluster: RelOptCluster, relOptTable: RelOptTable, hints: util.List[RelHint]): GeoMesaLogicalTableScan = {
    val geoMesaTable = relOptTable.unwrap(classOf[GeoMesaTranslatableTable])
    val traitSet = cluster
      .traitSetOf(Convention.NONE)
      .replaceIfs(RelCollationTraitDef.INSTANCE, () => {
        if (geoMesaTable != null) geoMesaTable.getStatistic.getCollations
        else ImmutableList.of[RelCollation]()
      })
    val emptyScanParams = ScanParams()
    new GeoMesaLogicalTableScan(cluster, traitSet, hints, relOptTable, geoMesaTable, emptyScanParams)
  }
}
