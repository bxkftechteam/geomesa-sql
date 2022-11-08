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

import org.apache.calcite.adapter.enumerable.{EnumerableRel, EnumerableRelImplementor, PhysTypeImpl}
import org.apache.calcite.linq4j.tree.{Blocks, Expressions}
import org.apache.calcite.plan._
import org.apache.calcite.rel.AbstractRelNode.sole
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterImpl
import org.apache.calcite.rel.metadata.RelMetadataQuery
import com.spatialx.geomesa.sql.GeoMesaTranslatableTable.GeoMesaQueryable

import java.{util => ju}

/**
  * Relational expression representing a query operation performed on GeoMesa datastore
  */
class GeoMesaToEnumerableConverter(cluster: RelOptCluster, traits: RelTraitSet, input: RelNode)
    extends ConverterImpl(cluster, ConventionTraitDef.INSTANCE, traits, input)
    with EnumerableRel {

  override def copy(traitSet: RelTraitSet, inputs: ju.List[RelNode]): RelNode =
    new GeoMesaToEnumerableConverter(cluster, traitSet, sole(inputs))

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost =
    super.computeSelfCost(planner, mq).multiplyBy(.1)

  override def implement(implementor: EnumerableRelImplementor, pref: EnumerableRel.Prefer): EnumerableRel.Result = {
    val physType = PhysTypeImpl.of(implementor.getTypeFactory, getRowType, pref.preferArray())
    val result = getInput.asInstanceOf[GeoMesaRel].implement()
    val table = result.table
    val queryParamsExpr = result.queryParams.toExpression()
    implementor.result(physType, Blocks.toBlock(
      Expressions.call(table.getExpression(classOf[GeoMesaQueryable[_]]), "query",
        implementor.getRootExpression,
        queryParamsExpr)))
  }
}
