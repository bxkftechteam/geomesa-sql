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

import org.apache.calcite.plan.RelOptRule

/**
  * Rules and relational operators for GeoMesaRel.CONVENTION calling
  * convension.
  */
object GeoMesaRules {
  val COMMON_LOGICAL_RULES: Seq[RelOptRule] = Seq(
    GeoMesaTableLogicalToPhysicalRule.SCAN.toRule,
    GeoMesaTableLogicalToPhysicalRule.MODIFY.toRule,
    GeoMesaTableLogicalToPhysicalRule.MODIFY_FALLBACK.toRule,
    GeoMesaFilterRule.CONFIG.toRule,
    GeoMesaProjectRule.CONFIG.toRule,
    GeoMesaLimitRule.CONFIG.toRule,
    GeoMesaIndexLookupJoinRule.CONFIG.toRule,
    EnumerableSpatialJoinRule.CONFIG.toRule
  )

  val AGGREGATION_RULE: RelOptRule = GeoMesaAggregateRule.CONFIG.toRule

  val PHYSICAL_RULES: Seq[RelOptRule] = Seq(
    GeoMesaToEnumerableConverterRule.DEFAULT_CONFIG.toRule
  )
}
