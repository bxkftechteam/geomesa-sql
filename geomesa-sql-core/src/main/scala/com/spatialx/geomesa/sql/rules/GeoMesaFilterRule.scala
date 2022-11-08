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

package com.spatialx.geomesa.sql.rules

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.{RelOptRuleCall, RelOptUtil, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rex.{RexNode, RexUtil}
import com.spatialx.geomesa.sql.nodes.GeoMesaLogicalTableScan
import org.locationtech.geomesa.filter.andFilters
import org.opengis.filter.{Filter => GTFilter}

import scala.collection.mutable.ArrayBuffer

/**
  * Rule handler for pushing filters down to GeoMesa table scan
  */
class GeoMesaFilterRule(config: GeoMesaRuleConfig) extends RelRule[GeoMesaRuleConfig](config) {
  import scala.collection.JavaConverters._

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter = call.rel[LogicalFilter](0)
    val tableScan = call.rel[GeoMesaLogicalTableScan](1)
    convert(filter, tableScan).foreach(call.transformTo(_))
  }

  def convert(filter: LogicalFilter, tableScan: GeoMesaLogicalTableScan): Option[RelNode] = {
    val sft = tableScan.geomesaTable.getSimpleFeatureType
    val conjunctions = RelOptUtil.conjunctions(filter.getCondition).asScala
    val gtFilters = ArrayBuffer.empty[GTFilter]
    val pushedConditions = ArrayBuffer.empty[RexNode]
    val remainingConditions = ArrayBuffer.empty[RexNode]
    conjunctions.foreach { condition =>
      val translator = new RexNodeTranslator(sft, tableScan.scanParams.columnIndexList)
      translator.rexNodeToGtFilter(condition) match {
        case Some(gtFilter) =>
          gtFilters += gtFilter
          pushedConditions += condition
        case None =>
          remainingConditions += condition
      }
    }
    val rexBuilder = filter.getCluster.getRexBuilder
    if (pushedConditions.isEmpty) None else {
      val tableScanWithFilter = tableScan.withFilter(
        rexBuilder,
        RexUtil.composeConjunction(rexBuilder, pushedConditions.asJava),
        andFilters(gtFilters))
      if (remainingConditions.isEmpty) Some(tableScanWithFilter) else {
        Some(new LogicalFilter(
          filter.getCluster,
          filter.getTraitSet,
          tableScanWithFilter,
          RexUtil.composeConjunction(rexBuilder, remainingConditions.asJava),
          ImmutableSet.copyOf(filter.getVariablesSet.iterator())))
      }
    }
  }
}

object GeoMesaFilterRule {
  val CONFIG = ImmutableGeoMesaRuleConfig.builder()
    .operandSupplier(
      b0 => b0.operand(classOf[LogicalFilter]).oneInput(
        b1 => b1.operand(classOf[GeoMesaLogicalTableScan]).predicate(!_.isAggregatedOrLimited).noInputs()))
    .ruleFactory(new GeoMesaFilterRule(_))
    .build()
}
