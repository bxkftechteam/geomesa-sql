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
import org.apache.calcite.plan.RelOptUtil.InputFinder
import org.apache.calcite.plan.{Convention, RelOptRule, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.util.ImmutableBitSet
import com.spatialx.geomesa.sql.nodes.EnumerableSpatialJoin
import com.spatialx.geomesa.sql.nodes.EnumerableSpatialJoin.SpatialIndexConfig
import com.spatialx.geomesa.sql.rules.EnumerableSpatialJoinRule.InputRefSides

import java.util

/**
 * Planner rule that converts a LogicalJoin relational expression to SpatialJoin, if the join condition is
 * a spatial predicate.
 */
class EnumerableSpatialJoinRule(config: Config) extends ConverterRule(config) {
  import scala.collection.JavaConverters._

  override def convert(rel: RelNode): RelNode = {
    val joinRel = rel.asInstanceOf[LogicalJoin]
    val joinType = joinRel.getJoinType
    if (joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT) {
      val newInputs = joinRel.getInputs.asScala.map { input =>
        input.getConvention match {
          case _: EnumerableConvention => input
          case _ => RelOptRule.convert(input, input.getTraitSet.replace(EnumerableConvention.INSTANCE))
        }
      }
      val left = newInputs.head
      val right = newInputs.last
      val condition = joinRel.getCondition
      analyzeSpatialJoinPredicate(joinRel, left, right, condition) match {
        case Some(spatialIndexConfig) =>
          EnumerableSpatialJoin.create(left, right, condition, spatialIndexConfig, joinRel.getVariablesSet, joinRel.getJoinType)
        case None => null
      }
    } else null
  }

  private def analyzeSpatialJoinPredicate(joinRel: RelNode, left: RelNode, right: RelNode, condition: RexNode): Option[SpatialIndexConfig] = {
    val predicates = RelOptUtil.conjunctions(condition).asScala
    predicates.flatMap(p => resolveSpatialJoinRexNodes(joinRel, left, right, p)).headOption
  }

  private def resolveSpatialJoinRexNodes(joinRel: RelNode, left: RelNode, right: RelNode, predicate: RexNode): Option[SpatialIndexConfig] = {
    predicate match {
      case rexCall: RexCall =>
        val operands = rexCall.getOperands
        rexCall.getOperator match {
          case udf: SqlUserDefinedFunction => udf.getName match {
            case "ST_CONTAINS" |
                 "ST_INTERSECTS" |
                 "ST_CROSSES" |
                 "ST_EQUALS" |
                 "ST_OVERLAPS" |
                 "ST_TOUCHES" |
                 "ST_WITHIN" => resolveSpatialJoinOperands(joinRel, left, right, operands)
            case "ST_DWITHIN" => resolveSpatialJoinOperandsForDWithin(joinRel, left, right, operands)
            case _ => None
          }
          case _ => None
        }
      case _ => None
    }
  }

  private def analyzeSideOfInputRefs(left: RelNode, right: RelNode, operand: RexNode): InputRefSides.Value = {
    val leftFieldCount = left.getRowType.getFieldCount
    val leftInputRefs = 0 until leftFieldCount
    val rightFieldCount = right.getRowType.getFieldCount
    val rightInputRefs = leftFieldCount until leftFieldCount + rightFieldCount
    val leftBitSet = ImmutableBitSet.of(leftInputRefs :_*)
    val rightBitSet = ImmutableBitSet.of(rightInputRefs :_*)
    val inputRefs = InputFinder.bits(operand)
    (inputRefs.intersects(leftBitSet), inputRefs.intersects(rightBitSet)) match {
      case (true, true) => InputRefSides.Both
      case (true, false) => InputRefSides.Left
      case (false, true) => InputRefSides.Right
      case (false, false) => InputRefSides.None
    }
  }

  private def resolveSpatialJoinOperands(joinRel: RelNode, left: RelNode, right: RelNode, operands: util.List[RexNode]): Option[SpatialIndexConfig] = {
    val operandInputRefSide0 = analyzeSideOfInputRefs(left, right, operands.get(0))
    val operandInputRefSide1 = analyzeSideOfInputRefs(left, right, operands.get(1))
    (operandInputRefSide0, operandInputRefSide1) match {
      case (InputRefSides.Left, InputRefSides.Right) =>
        Some(SpatialIndexConfig(operands.get(0), rewriteInputRefsForRightRel(joinRel, left, right, operands.get(1))))
      case (InputRefSides.Right, InputRefSides.Left) =>
        Some(SpatialIndexConfig(operands.get(1), rewriteInputRefsForRightRel(joinRel, left, right, operands.get(0))))
      case _ => None
    }
  }

  private def resolveSpatialJoinOperandsForDWithin(joinRel: RelNode, left: RelNode, right: RelNode, operands: util.List[RexNode]): Option[SpatialIndexConfig] = {
    operands.get(2) match {
      case distance: RexLiteral =>
        val within = distance.getValueAs(classOf[java.lang.Double])
        val spatialIndexConfig = resolveSpatialJoinOperands(joinRel, left, right, operands)
        spatialIndexConfig.map(_.copy(within = within))
      case _ => None
    }
  }

  private def rewriteInputRefsForRightRel(joinRel: RelNode, left: RelNode, right: RelNode, operand: RexNode): RexNode = {
    val rexBuilder = left.getCluster.getRexBuilder
    val adjustments = new Array[Int](joinRel.getRowType.getFieldCount)
    val adjustment = left.getRowType.getFieldCount
    (adjustment until adjustments.length).foreach(adjustments(_) = -adjustment)
    val rexInputConverter = new RelOptUtil.RexInputConverter(rexBuilder, joinRel.getRowType.getFieldList, right.getRowType.getFieldList, adjustments)
    operand.accept(rexInputConverter)
  }
}

object EnumerableSpatialJoinRule {
  val CONFIG = Config.INSTANCE
    .withConversion(classOf[LogicalJoin], Convention.NONE, EnumerableConvention.INSTANCE, "SpatialJoinRule")
    .withRuleFactory(new EnumerableSpatialJoinRule(_))

  object InputRefSides extends Enumeration {
    val Left, Right, Both, None = Value
  }
}
