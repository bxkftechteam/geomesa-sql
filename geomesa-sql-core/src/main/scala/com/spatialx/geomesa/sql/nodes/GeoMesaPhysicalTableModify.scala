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

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable._
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.tree.{BlockBuilder, Expression, Expressions, FunctionExpression}
import org.apache.calcite.plan._
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.AbstractRelNode.sole
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexInputRef, RexNode, RexProgramBuilder}
import org.apache.calcite.util.BuiltInMethod
import com.spatialx.geomesa.sql.{FID_FIELD_NAME, GeoMesaTranslatableTable}
import com.spatialx.geomesa.sql.modifier.GeoMesaTableModifier
import com.spatialx.geomesa.sql.modifier.GeoMesaTableModifier.TableModifyOperationType
import com.spatialx.geomesa.sql.nodes.GeoMesaPhysicalTableModify.modifyTableMethod

import java.lang.reflect.Method
import java.util
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Physical plan node for modifying GeoMesa table, which is converted from GeoMesaLogicalTableModify
 */
class GeoMesaPhysicalTableModify(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable,
                                 schema: Prepare.CatalogReader, input: RelNode, operation: TableModify.Operation,
                                 updateColumnList: util.List[String], sourceExpressionList: util.List[RexNode],
                                 flattened: Boolean)
  extends TableModify(cluster, traitSet, table, schema, input, operation, updateColumnList,
    sourceExpressionList, flattened) with EnumerableRel {

  import scala.collection.JavaConverters._

  assert(input.getConvention.isInstanceOf[EnumerableConvention])
  assert(getConvention.isInstanceOf[EnumerableConvention])
  val geomesaTable: GeoMesaTranslatableTable = table.unwrap(classOf[GeoMesaTranslatableTable])

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost =
    super.computeSelfCost(planner, mq).multiplyBy(0.5)

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode =
    new GeoMesaPhysicalTableModify(getCluster, traitSet, getTable, getCatalogReader, sole(inputs), getOperation,
      getUpdateColumnList, getSourceExpressionList, isFlattened)

  override def implement(implementor: EnumerableRelImplementor, pref: EnumerableRel.Prefer): EnumerableRel.Result = {
    val builder = new BlockBuilder
    val physType = PhysTypeImpl.of(implementor.getTypeFactory, getRowType, pref.preferArray())
    val geomesaTableExpr = table.getExpression(classOf[GeoMesaTranslatableTable])
    val childResult = implementor.visitChild(this, 0, getInput.asInstanceOf[EnumerableRel], pref)
    val childExpr = builder.append("child", childResult.block)
    val projectedChildExpr = projectChildExpression(implementor, builder, childResult, childExpr, pref)
    val opType = getOperation match {
      case TableModify.Operation.INSERT => TableModifyOperationType.INSERT
      case TableModify.Operation.UPDATE => TableModifyOperationType.UPDATE
      case TableModify.Operation.DELETE => TableModifyOperationType.DELETE
      case op => throw new UnsupportedOperationException(s"Table modify operation $op is not supported")
    }
    val modifyTableExpr = builder.append("count", Expressions.call(
      modifyTableMethod,
      implementor.getRootExpression,
      geomesaTableExpr,
      projectedChildExpr,
      Expressions.constant(opType)));
    builder.add(
      Expressions.return_(
        null,
        Expressions.call(
          BuiltInMethod.SINGLETON_ENUMERABLE.method,
          modifyTableExpr)))
    implementor.result(physType, builder.toBlock)
  }

  private def projectChildExpression(implementor: EnumerableRelImplementor, builder: BlockBuilder,
                                     childResult: EnumerableRel.Result, childExp: Expression,
                                     pref: EnumerableRel.Prefer): Expression = {
    if (updateColumnList == null || updateColumnList.isEmpty) childExp else {
      if (updateColumnList.contains(FID_FIELD_NAME) && getOperation == TableModify.Operation.UPDATE) {
        throw new UnsupportedOperationException("Cannot change feature ID in UPDATE statement")
      }
      val childProjector = buildChildExpressionProjector(implementor, childResult.physType, pref)
      builder.append("convertedChild", Expressions.call(childExp, BuiltInMethod.SELECT.method, childProjector))
    }
  }

  private def buildChildExpressionProjector(implementor: EnumerableRelImplementor,
                                      childPhysType: PhysType,
                                      pref: EnumerableRel.Prefer): FunctionExpression[_] = {
    val blockBuilder = new BlockBuilder
    val inputParam = Expressions.parameter(childPhysType.getJavaRowType, "input")
    val typeFactory = implementor.getTypeFactory
    val inputRowType = getInput.getRowType
    val tableRowType = getTable.getRowType
    val tablePhysType = PhysTypeImpl.of(typeFactory, tableRowType, pref.preferArray())
    val program = new RexProgramBuilder(input.getRowType, getCluster.getRexBuilder)
    tableRowType.getFieldNames.asScala.zipWithIndex.foreach { case (fieldName, tableFieldIndex) =>
      val expression = updateColumnList.indexOf(fieldName) match {
        case -1 => RexInputRef.of(tableFieldIndex, inputRowType)
        case k => sourceExpressionList.get(k)
      }
      program.addProject(tableFieldIndex, expression, fieldName)
    }
    val inputGetter = new RexToLixTranslator.InputGetterImpl(inputParam, childPhysType)
    val exprs = RexToLixTranslator.translateProjects(
      program.getProgram, typeFactory, implementor.getConformance, blockBuilder, null, tablePhysType,
      DataContext.ROOT,
      inputGetter,
      implementor.getCorrelVariableGetter)
    blockBuilder.add(Expressions.return_(null, tablePhysType.record(exprs)))
    Expressions.lambda(blockBuilder.toBlock, inputParam)
  }
}

object GeoMesaPhysicalTableModify {
  val modifyTableMethod: Method = classOf[GeoMesaPhysicalTableModify].getDeclaredMethods
    .find(m => m.getName == "modifyTable").get

  def modifyTable(root: DataContext, geomesaTable: GeoMesaTranslatableTable, source: Enumerable[AnyRef],
                  opType: Int): Long = {
    val cancelFlag = DataContext.Variable.CANCEL_FLAG.get[AtomicBoolean](root)
    val modifier = new GeoMesaTableModifier(cancelFlag, geomesaTable, source, opType)
    try {
      modifier.modifyTable()
    } finally {
      modifier.close()
    }
  }
}
