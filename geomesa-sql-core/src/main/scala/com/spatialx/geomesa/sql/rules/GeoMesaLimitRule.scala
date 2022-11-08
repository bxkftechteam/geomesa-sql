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

import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rex.{RexLiteral, RexNode}
import com.spatialx.geomesa.sql.nodes.GeoMesaLogicalTableScan

import scala.util.Try

/**
  * Rule handler for pushing limit clause down to GeoMesa table scan
  */
class GeoMesaLimitRule(config: GeoMesaRuleConfig) extends RelRule[GeoMesaRuleConfig](config) {
  override def onMatch(call: RelOptRuleCall): Unit = {
    val logicalSortRel = call.rel(0).asInstanceOf[LogicalSort]
    val logicalTableScan = call.rel(1).asInstanceOf[GeoMesaLogicalTableScan]
    assert(logicalSortRel.getSortExps.isEmpty)
    val offsetOpt = if (logicalSortRel.offset == null) Some(0) else rexNodeToIntValue(logicalSortRel.offset)
    val fetchOpt = if (logicalSortRel.fetch == null) Some(0) else rexNodeToIntValue(logicalSortRel.fetch)
    (offsetOpt, fetchOpt) match {
      case (Some(0), Some(0)) => ()
      case (Some(offset), Some(fetch)) => call.transformTo(convert(logicalTableScan, offset, fetch))
      case _ => ()
    }
  }

  private def convert(logicalTableScan: GeoMesaLogicalTableScan, offset: Long, fetch: Long): RelNode = {
    logicalTableScan.withLimit(offset, fetch)
  }

  private def rexNodeToIntValue(rexNode: RexNode): Option[Int] = {
    Try(RexLiteral.intValue(rexNode)).toOption
  }
}

object GeoMesaLimitRule {
  val CONFIG = ImmutableGeoMesaRuleConfig.builder()
    .operandSupplier(b0 => b0.operand(classOf[LogicalSort]).predicate(_.getSortExps.isEmpty).oneInput(
      b1 => b1.operand(classOf[GeoMesaLogicalTableScan]).predicate(!_.isLimited).anyInputs()))
    .ruleFactory(new GeoMesaLimitRule(_))
    .build()
}
