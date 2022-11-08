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

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.plan._
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.AbstractRelNode.sole
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rex.RexNode
import com.spatialx.geomesa.sql.rules.GeoMesaRules

import java.util

/**
 * Logical plan node for modifying GeoMesa table
 */
class GeoMesaLogicalTableModify(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable,
                                schema: Prepare.CatalogReader, input: RelNode, operation: TableModify.Operation,
                                updateColumnList: util.List[String], sourceExpressionList: util.List[RexNode],
                                flattened: Boolean)
  extends TableModify(cluster, traitSet, table, schema, input, operation, updateColumnList,
    sourceExpressionList, flattened) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    assert(traitSet.containsIfApplicable(Convention.NONE))
    new GeoMesaLogicalTableModify(getCluster, traitSet, table, catalogReader,
      sole(inputs), getOperation, getUpdateColumnList,
      getSourceExpressionList, isFlattened)
  }

  override def register(planner: RelOptPlanner): Unit =
    GeoMesaRules.COMMON_LOGICAL_RULES.foreach(planner.addRule)

  def toPhysical(input: RelNode): GeoMesaPhysicalTableModify = {
    val physicalTraitSet = traitSet.replace(EnumerableConvention.INSTANCE)
    val convertedInput = RelOptRule.convert(input, physicalTraitSet)
    new GeoMesaPhysicalTableModify(getCluster, physicalTraitSet, table, catalogReader,
      convertedInput, getOperation, getUpdateColumnList,
      getSourceExpressionList, isFlattened)
  }
}

object GeoMesaLogicalTableModify {
  def create(table: RelOptTable,
             schema: Prepare.CatalogReader, input: RelNode,
             operation: TableModify.Operation, updateColumnList: util.List[String],
             sourceExpressionList: util.List[RexNode], flattened: Boolean): GeoMesaLogicalTableModify = {
    val cluster = input.getCluster
    val traitSet = cluster.traitSetOf(Convention.NONE)
    new GeoMesaLogicalTableModify(cluster, traitSet, table, schema, input, operation, updateColumnList,
      sourceExpressionList, flattened)
  }
}
