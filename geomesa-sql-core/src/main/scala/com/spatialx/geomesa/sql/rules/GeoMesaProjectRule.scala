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
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexInputRef
import com.spatialx.geomesa.sql.nodes.GeoMesaLogicalTableScan

/**
  * Rule for pushing projection down to GeoMesaTableScan
  */
class GeoMesaProjectRule(config: GeoMesaRuleConfig) extends RelRule[GeoMesaRuleConfig](config) {
  import scala.collection.JavaConverters._

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project = call.rel[LogicalProject](0)
    val tableScan = call.rel[GeoMesaLogicalTableScan](1)
    convert(project, tableScan).foreach(call.transformTo(_))
  }

  def convert(project: LogicalProject, tableScan: GeoMesaLogicalTableScan): Option[GeoMesaLogicalTableScan] = {
    val projects = project.getProjects
    val indexList = projects.asScala.flatMap { rexNode =>
      rexNode match {
        case ref: RexInputRef => Some(ref.getIndex)
        case _ => None
      }
    }
    if (indexList.size != projects.size()) None else {
      Some(tableScan.withProject(project.getRowType, indexList))
    }
  }
}

object GeoMesaProjectRule {
  val CONFIG = ImmutableGeoMesaRuleConfig.builder()
    .operandSupplier(
      b0 => b0.operand(classOf[LogicalProject]).oneInput(
        b1 => b1.operand(classOf[GeoMesaLogicalTableScan]).predicate(!_.isAggregatedOrLimited).noInputs()))
    .ruleFactory(new GeoMesaProjectRule(_))
    .build()
}
