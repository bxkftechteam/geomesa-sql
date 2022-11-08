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
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.linq4j.function.Predicate2
import org.apache.calcite.linq4j.tree.{BlockBuilder, Expressions, FunctionExpression}
import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinRelType}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.{RelMdCollation, RelMetadataQuery}
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.rex.{RexNode, RexProgramBuilder}
import org.apache.calcite.sql.`type`.SqlTypeName
import com.spatialx.geomesa.sql.GeomLambdaExpression
import com.spatialx.geomesa.sql.enumerator.EnumerableSpatialJoinEnumerator
import com.spatialx.geomesa.sql.nodes.EnumerableSpatialJoin.{SpatialIndexConfig, spatialJoinMethod}

import java.lang.reflect.Method
import java.util
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Implementation of join optimized for joining on spatial predicates.
 */
class EnumerableSpatialJoin(cluster: RelOptCluster,
                            traits: RelTraitSet,
                            hints: util.List[RelHint],
                            left: RelNode,
                            right: RelNode,
                            condition: RexNode,
                            spatialIndexConfig: SpatialIndexConfig,
                            variablesSet: util.Set[CorrelationId],
                            joinType: JoinRelType)
  extends Join(cluster, traits, hints, left, right, condition, variablesSet, joinType)
  with EnumerableRel {

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw)
      .item("leftGeom", spatialIndexConfig.left)
      .item("rightGeom", spatialIndexConfig.right)
      .item("within", spatialIndexConfig.within)

  override def copy(traitSet: RelTraitSet, conditionExpr: RexNode, left: RelNode, right: RelNode, joinType: JoinRelType, semiJoinDone: Boolean): Join = {
    new EnumerableSpatialJoin(cluster, traitSet, getHints, left, right, conditionExpr, spatialIndexConfig, getVariablesSet, joinType)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    super.computeSelfCost(planner, mq).multiplyBy(1.1)
  }

  override def implement(implementor: EnumerableRelImplementor, pref: EnumerableRel.Prefer): EnumerableRel.Result = {
    val builder = new BlockBuilder
    val leftResult = implementor.visitChild(this, 0, getLeft.asInstanceOf[EnumerableRel], pref)
    val rightResult = implementor.visitChild(this, 1, getRight.asInstanceOf[EnumerableRel], pref)
    val leftExpression = builder.append("left", leftResult.block)
    val rightExpression = builder.append("right", rightResult.block)
    val physType = PhysTypeImpl.of(implementor.getTypeFactory, getRowType, pref.preferArray())
    val resultSelector = ExtendedEnumUtils.joinSelector(joinType, physType, leftResult.physType, rightResult.physType)
    val predicate = ExtendedEnumUtils.generatePredicate(implementor, getCluster.getRexBuilder, getLeft, getRight,
      leftResult.physType, rightResult.physType, condition)
    val leftGeomEvaluator = spatialOperandExpression(implementor, spatialIndexConfig.left, getLeft, leftResult.physType, pref)
    val rightGeomEvaluator = spatialOperandExpression(implementor, spatialIndexConfig.right, getRight, rightResult.physType, pref)
    builder.append(Expressions.call(
      spatialJoinMethod,
      implementor.getRootExpression,
      leftExpression,
      rightExpression,
      resultSelector,
      leftGeomEvaluator,
      rightGeomEvaluator,
      Expressions.constant(spatialIndexConfig.within),
      Expressions.constant(joinType.generatesNullsOnRight()),
      predicate
    ))
    implementor.result(physType, builder.toBlock)
  }

  private def spatialOperandExpression(implementor: EnumerableRelImplementor, geomRexNode: RexNode,
                                       input: RelNode,
                                       physType: PhysType,
                                       pref: EnumerableRel.Prefer): FunctionExpression[_] = {
    val blockBuilder = new BlockBuilder
    val inputParam = Expressions.parameter(physType.getJavaRowType, "input")
    val typeFactory = implementor.getTypeFactory
    val program = new RexProgramBuilder(input.getRowType, getCluster.getRexBuilder)
    program.addProject(0, geomRexNode, "geom")
    val geomsPhysType = PhysTypeImpl.of(typeFactory, typeFactory.createStructType(
      ImmutableList.of(typeFactory.createSqlType(SqlTypeName.GEOMETRY)),
      ImmutableList.of("geom")), pref.preferArray())
    val inputGetter = new RexToLixTranslator.InputGetterImpl(inputParam, physType)
    val geomExpressions = RexToLixTranslator.translateProjects(
      program.getProgram, typeFactory, implementor.getConformance, blockBuilder, null, geomsPhysType,
      DataContext.ROOT,
      inputGetter,
      implementor.getCorrelVariableGetter)
    blockBuilder.add(Expressions.return_(null, geomsPhysType.record(geomExpressions)))
    GeomLambdaExpression.lambda(blockBuilder.toBlock, inputParam)
  }
}

object EnumerableSpatialJoin {

  def create(left: RelNode, right: RelNode, condition: RexNode, spatialIndexConfig: SpatialIndexConfig,
             variablesSet: util.Set[CorrelationId],
             joinType: JoinRelType): EnumerableSpatialJoin = {
    val cluster = left.getCluster
    val mq = cluster.getMetadataQuery
    val traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE).replaceIfs(
      RelCollationTraitDef.INSTANCE, () => RelMdCollation.enumerableHashJoin(mq, left, right, joinType))
    new EnumerableSpatialJoin(cluster, traitSet, ImmutableList.of[RelHint], left, right, condition, spatialIndexConfig,
      variablesSet, joinType)
  }

  val spatialJoinMethod: Method = classOf[EnumerableSpatialJoin].getDeclaredMethods.find(m => m.getName == "spatialJoin").get

  /**
   * Config for building and querying spatial indexes for speeding up spatial join
   * @param left expression evaluates to geometries for rows in left side relation
   * @param right expression evaluates to geometries for rows in right side relation
   * @param within extend the query geometry to support dwithin join
   */
  case class SpatialIndexConfig(
    left: RexNode,
    right: RexNode,
    within: Double = 0.0)

  def spatialJoin(root: DataContext,
                  left: Enumerable[AnyRef],
                  right: Enumerable[AnyRef],
                  resultSelector: org.apache.calcite.linq4j.function.Function2[AnyRef, AnyRef, AnyRef],
                  leftGeomEvaluator: GeomLambdaExpression.GeomFunction1[AnyRef],
                  rightGeomEvaluator: GeomLambdaExpression.GeomFunction1[AnyRef],
                  within: Double,
                  generateNullsOnRight: Boolean,
                  predicate: Predicate2[AnyRef, AnyRef]): Enumerable[AnyRef] = {
    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get[AtomicBoolean](root)
    new AbstractEnumerable[AnyRef] {
      override def enumerator(): Enumerator[AnyRef] = {
        new EnumerableSpatialJoinEnumerator(cancelFlag, left, right, resultSelector, leftGeomEvaluator, rightGeomEvaluator,
          within, generateNullsOnRight, predicate)
      }
    }
  }
}
