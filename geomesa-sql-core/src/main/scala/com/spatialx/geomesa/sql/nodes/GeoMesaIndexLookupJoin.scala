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
import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable._
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.function.{EqualityComparer, Function1, Predicate2}
import org.apache.calcite.linq4j.tree.{BlockBuilder, Expressions}
import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinRelType}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.{RelMdCollation, RelMetadataQuery}
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.RexNode
import com.spatialx.geomesa.sql.GeoMesaQueryParams
import com.spatialx.geomesa.sql.GeoMesaTranslatableTable.GeoMesaQueryable
import com.spatialx.geomesa.sql.enumerator.GeoMesaIndexLookupJoinEnumerable
import com.spatialx.geomesa.sql.nodes.GeoMesaIndexLookupJoin.indexLookupJoinMethod

import java.lang.reflect.Method
import java.util
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Index lookup join operator for GeoTools datastore
 */
class GeoMesaIndexLookupJoin(cluster: RelOptCluster, traits: RelTraitSet, hints: util.List[RelHint],
                             left: RelNode, right: RelNode, condition: RexNode,
                             variablesSet: util.Set[CorrelationId], joinType: JoinRelType)
  extends Join(cluster, traits, hints, left, right, condition, variablesSet, joinType) with EnumerableRel {

  import scala.collection.JavaConverters._

  override def copy(traitSet: RelTraitSet, conditionExpr: RexNode, left: RelNode, right: RelNode,
                    joinType: JoinRelType, semiJoinDone: Boolean): Join =
    new GeoMesaIndexLookupJoin(cluster, traitSet, getHints, left, right, conditionExpr, getVariablesSet, joinType)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost =
    super.computeSelfCost(planner, mq).multiplyBy(0.9)

  /**
   * Generate code for performing index lookup join. Significant portion of this code is taken from EnumerableHashJoin
   */
  override def implement(implementor: EnumerableRelImplementor, pref: EnumerableRel.Prefer): EnumerableRel.Result = {
    val physType = PhysTypeImpl.of(implementor.getTypeFactory, getRowType, pref.preferArray())
    val leftResult = implementor.visitChild(this, 0, getLeft.asInstanceOf[EnumerableRel], pref)
    val rightResult = getRight.asInstanceOf[GeoMesaRel].implement()
    val builder = new BlockBuilder
    val leftKeysSelector = leftResult.physType.generateAccessor(joinInfo.leftKeys)
    val leftExpression = builder.append("left", leftResult.block)
    val rightExpression = rightResult.queryParams.toExpression()
    val rightQueryable = rightResult.table.getExpression(classOf[GeoMesaQueryable[_]])
    val rightKeysExpression = Expressions.newArrayInit(classOf[Int],
      joinInfo.rightKeys.toIntArray.map(Expressions.constant(_)).toList.asJava)
    val keyPhysType = leftResult.physType.project(joinInfo.leftKeys, JavaRowFormat.LIST)
    if (keyPhysType.comparer() != null) {
      throw new IllegalStateException(
        s"equi-join on non-trivial key types is not supported by GeoMesaIndexLookupJoin. keyPhysType: $keyPhysType")
    }
    val rightPhysType = PhysTypeImpl.of(implementor.getTypeFactory, getRight.getRowType, pref.preferArray())
    val rightKeysSelector = rightPhysType.generateAccessor(joinInfo.rightKeys)
    val resultSelector = ExtendedEnumUtils.joinSelector(joinType, physType, leftResult.physType, rightPhysType)
    val equalityComparator = Option(keyPhysType.comparer()).getOrElse(Expressions.constant(null))
    val predicate = ExtendedEnumUtils.generatePredicate(implementor, getCluster.getRexBuilder, getLeft, getRight,
        leftResult.physType, rightPhysType, condition)

    builder.append(Expressions.call(
      indexLookupJoinMethod,
      implementor.getRootExpression,
      leftExpression,
      leftKeysSelector,
      rightExpression,
      rightQueryable,
      rightKeysExpression,
      rightKeysSelector,
      resultSelector,
      equalityComparator,
      Expressions.constant(joinType.generatesNullsOnLeft()),
      Expressions.constant(joinType.generatesNullsOnRight()),
      predicate
    ))
    implementor.result(physType, builder.toBlock)
  }
}

object GeoMesaIndexLookupJoin {

  def create(left: RelNode, right: RelNode, condition: RexNode, variablesSet: util.Set[CorrelationId],
             joinType: JoinRelType): GeoMesaIndexLookupJoin = {
    val cluster = left.getCluster
    val mq = cluster.getMetadataQuery
    val traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE).replaceIfs(
      RelCollationTraitDef.INSTANCE, () => RelMdCollation.enumerableHashJoin(mq, left, right, joinType))
    new GeoMesaIndexLookupJoin(cluster, traitSet, ImmutableList.of[RelHint], left, right, condition, variablesSet, joinType)
  }

  val indexLookupJoinMethod: Method = classOf[GeoMesaIndexLookupJoin].getDeclaredMethods.find(m => m.getName == "indexLookupJoin").get

  def indexLookupJoin(root: DataContext,
                      left: Enumerable[AnyRef],
                      leftKeysSelector: Function1[AnyRef, AnyRef],
                      right: GeoMesaQueryParams,
                      rightQueryable: GeoMesaQueryable[AnyRef],
                      rightKeys: Array[Int],
                      rightKeysSelector: Function1[AnyRef, AnyRef],
                      resultSelector: org.apache.calcite.linq4j.function.Function2[AnyRef, AnyRef, AnyRef],
                      comparer: EqualityComparer[AnyRef],
                      generateNullsOnLeft: Boolean,
                      generateNullsOnRight: Boolean,
                      predicate: Predicate2[AnyRef, AnyRef]): Enumerable[AnyRef] = {
    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get[AtomicBoolean](root)
    new GeoMesaIndexLookupJoinEnumerable(cancelFlag, left, leftKeysSelector, right, rightQueryable, rightKeys,
      rightKeysSelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate)
  }
}
