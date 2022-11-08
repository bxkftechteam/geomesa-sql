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
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalJoin
import com.spatialx.geomesa.sql.nodes.{GeoMesaIndexLookupJoin, GeoMesaLogicalTableScan, GeoMesaRel, GeoMesaTableScan}
import org.locationtech.jts.geom.Geometry

/**
 * Perform equijoin by looking up left side values from right side table
 */
class GeoMesaIndexLookupJoinRule(config: GeoMesaRuleConfig) extends RelRule[GeoMesaRuleConfig](config) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel(0).asInstanceOf[LogicalJoin]
    val tableScan = call.rel(2).asInstanceOf[GeoMesaTableScan]
    val joinType = join.getJoinType
    if (joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT) {
      // Index lookup join can only be performed on equijoin
      val info = join.analyzeCondition()
      val hasEquiKeys = !info.leftKeys.isEmpty && !info.rightKeys.isEmpty
      if (hasEquiKeys && isEquiKeysIndexed(tableScan, info.rightKeys.toIntArray)) {
        call.transformTo(convert(join, tableScan))
      }
    }
  }

  private def convert(join: LogicalJoin, tableScan: GeoMesaTableScan): GeoMesaIndexLookupJoin = {
    val left = join.getInput(0)
    val convertedLeft = left.getConvention match {
      case _: EnumerableConvention => left
      case _ => RelOptRule.convert(left, left.getTraitSet.replace(EnumerableConvention.INSTANCE))
    }
    val physicalTableScan = RelOptRule.convert(tableScan, tableScan.getTraitSet.replace(GeoMesaRel.CONVENTION))
    GeoMesaIndexLookupJoin.create(convertedLeft, physicalTableScan, join.getCondition, join.getVariablesSet, join.getJoinType)
  }

  private def isEquiKeysIndexed(tableScan: GeoMesaTableScan, keys: Seq[Int]): Boolean = {
    val project = tableScan.scanParams.columnIndexList
    val sft = tableScan.geomesaTable.getSimpleFeatureType
    val attrIndexSpecs = getTableIndexSpecs(tableScan)
    keys.exists { key =>
      val attrIndex = if (project.nonEmpty) project(key) else key
      if (attrIndex == 0) true else {
        val attrDesc = sft.getDescriptor(attrIndex - 1)
        val attrName = attrDesc.getLocalName
        val attrBinding = attrDesc.getType.getBinding
        !isUnsupportedType(attrBinding) && attrIndexSpecs.exists(spec => spec(0) == attrName)
      }
    }
  }

  private def getTableIndexSpecs(tableScan: GeoMesaTableScan): Array[Array[String]] = {
    val sft = tableScan.geomesaTable.getSimpleFeatureType
    val indexSpecs = sft.getUserData.getOrDefault("geomesa.indices", "").asInstanceOf[String]
    indexSpecs.split(",").flatMap { spec =>
      val comps = spec.split(":")
      if (comps.length >= 4 && comps(0) == "attr") {
        Some(comps.slice(3, comps.length))
      } else None
    }
  }

  private def isUnsupportedType(binding: Class[_]): Boolean =
    binding == classOf[Geometry] || binding == classOf[Array[Byte]]
}

object GeoMesaIndexLookupJoinRule {
  val CONFIG = ImmutableGeoMesaRuleConfig.builder()
    .operandSupplier(b0 => b0.operand(classOf[LogicalJoin]).inputs(
      b1 => b1.operand(classOf[RelNode]).anyInputs(),
      b2 => b2.operand(classOf[GeoMesaLogicalTableScan]).predicate(!_.isAggregatedOrLimited).noInputs()))
    .ruleFactory(new GeoMesaIndexLookupJoinRule(_))
    .build()
}
