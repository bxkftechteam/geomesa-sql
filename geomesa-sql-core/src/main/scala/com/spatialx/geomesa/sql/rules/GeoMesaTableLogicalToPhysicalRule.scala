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

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.plan.{Convention, RelOptRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.logical.LogicalTableModify
import com.spatialx.geomesa.sql.nodes.{GeoMesaLogicalTableModify, GeoMesaLogicalTableScan, GeoMesaPhysicalTableModify, GeoMesaRel}

/**
 * Rule for converting GeoMesaLogicalTableScan to GeoMesaPhysicalTableScan
 */
class GeoMesaTableLogicalToPhysicalScanRule(config: Config) extends ConverterRule(config) {
  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[GeoMesaLogicalTableScan]
    scan.toPhysical
  }
}

/**
 * Rule for converting GeoMesaLogicalTableModify to GeoMesaPhysicalTableModify
 */
class GeoMesaTableLogicalToPhysicalModifyRule(config: ConverterRule.Config) extends ConverterRule(config) {
  def convert(rel: RelNode): RelNode = {
    val modify = rel.asInstanceOf[GeoMesaLogicalTableModify]
    val input = modify.getInput(0)
    modify.toPhysical(input)
  }
}

/**
 * Calcite does not generate GeoMesaLogicalTableModify node when performing delete or update, so we need this
 * fallback rule for converting LogicalTableModify node to GeoMesaPhysicalTableModify node.
 */
class GeoMesaTablePhysicalModifyRule(config: ConverterRule.Config) extends ConverterRule(config) {
  def convert(rel: RelNode): RelNode = {
    val modify = rel.asInstanceOf[LogicalTableModify]
    val input = modify.getInput(0)
    val traitSet = modify.getTraitSet.replace(EnumerableConvention.INSTANCE)
    val convertedInput = RelOptRule.convert(input, traitSet)
    new GeoMesaPhysicalTableModify(
      modify.getCluster, traitSet,
      modify.getTable,
      modify.getCatalogReader,
      convertedInput,
      modify.getOperation,
      modify.getUpdateColumnList,
      modify.getSourceExpressionList,
      modify.isFlattened)
  }
}

object GeoMesaTableLogicalToPhysicalRule {
  val SCAN = Config.INSTANCE
    .withConversion(classOf[GeoMesaLogicalTableScan],
      Convention.NONE, GeoMesaRel.CONVENTION, "GeoMesaTableLogicalToPhysicalScanRule")
  .withRuleFactory(new GeoMesaTableLogicalToPhysicalScanRule(_))
  val MODIFY = Config.INSTANCE
    .withConversion(classOf[GeoMesaLogicalTableModify],
      Convention.NONE, EnumerableConvention.INSTANCE, "GeoMesaTableLogicalToPhysicalModifyRule")
    .withRuleFactory(new GeoMesaTableLogicalToPhysicalModifyRule(_))
  val MODIFY_FALLBACK = Config.INSTANCE
    .withConversion(classOf[LogicalTableModify],
      Convention.NONE, EnumerableConvention.INSTANCE, "GeoMesaTablePhysicalModifyRule")
    .withRuleFactory(new GeoMesaTablePhysicalModifyRule(_))
}
