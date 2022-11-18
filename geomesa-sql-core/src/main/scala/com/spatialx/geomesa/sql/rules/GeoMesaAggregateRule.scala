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
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.{SqlCountAggFunction, SqlMinMaxAggFunction}
import com.spatialx.geomesa.sql.nodes.GeoMesaLogicalTableScan
import org.locationtech.geomesa.utils.stats.Stat
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeatureType

import java.util.UUID

/**
  * Rule for pushing down aggregation to GeoMesa table scan
  */
class GeoMesaAggregateRule(config: GeoMesaRuleConfig) extends RelRule[GeoMesaRuleConfig](config) {
  import scala.collection.JavaConverters._

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rel(0).asInstanceOf[LogicalAggregate]
    val tableScan = call.rel(1).asInstanceOf[GeoMesaLogicalTableScan]
    val groupSet = agg.getGroupSet
    val cardinality = groupSet.cardinality()
    val sft = tableScan.geomesaTable.getSimpleFeatureType
    val projection = tableScan.scanParams.columnIndexList
    val groupedColumnId = if (cardinality == 1) Some(groupSet.nth(0)) else None
    val aggCalls = agg.getAggCallList.asScala
    if (aggCalls.isEmpty && groupedColumnId.isDefined) {
      // GROUP BY without aggregation function, will run as an EnumerationStat query
      val statsSpecs = enumStatSpecs(groupedColumnId, sft, projection)
      if (statsSpecs.nonEmpty) {
        call.transformTo(convert(tableScan, agg.getRowType, statsSpecs))
      }
    } else {
      val statsSpecs = aggCallsToStatsSpecs(groupedColumnId, aggCalls, sft, projection)
      if (statsSpecs.size == aggCalls.size) {
        call.transformTo(convert(tableScan, agg.getRowType, statsSpecs))
      }
    }
  }

  private def convert(logicalTableScan: GeoMesaLogicalTableScan, aggResultRowType: RelDataType, statsSpecs: Seq[(String, String)]): RelNode = {
    val statsStrings = statsSpecs.map(_._1)
    val statAttributes = statsSpecs.map(_._2)
    logicalTableScan.withAggregate(aggResultRowType, statsStrings, statAttributes)
  }

  private def enumStatSpecs(groupedColumnId: Option[Int], sft: SimpleFeatureType, projection: Seq[Int]): Seq[(String, String)] = {
    val groupByAttribute = resolveGroupedAttribute(groupedColumnId, sft, projection)
    groupByAttribute match {
      case Some(groupedAttributeName) => Seq((Stat.Enumeration(groupedAttributeName), ""))
      case None => Seq.empty
    }
  }

  private def aggCallsToStatsSpecs(groupedColumnId: Option[Int], aggCalls: Seq[AggregateCall],
                                    sft: SimpleFeatureType, projection: Seq[Int]): Seq[(String, String)] = {
    val project = (ordinal: Int) => if (projection.nonEmpty) projection(ordinal) else ordinal
    val groupByAttribute = resolveGroupedAttribute(groupedColumnId, sft, projection)
    groupByAttribute match {
      case Some(groupedAttributeName) => aggCalls.flatMap { call =>
        aggCallToStatSpec(call, sft, project) match {
          case Some((statString, statProperty)) =>
            if (groupedAttributeName.isEmpty) Some(statString, statProperty) else {
              Some(Stat.GroupBy(groupedAttributeName, statString), statProperty)
            }
          case None => None
        }
      }
      case None => Seq.empty
    }
  }

  private def aggCallToStatSpec(call: AggregateCall, sft: SimpleFeatureType, project: Int => Int): Option[(String, String)] = {
    call.getAggregation match {
      case _: SqlCountAggFunction if !call.isDistinct && call.getArgList.isEmpty => Some(Stat.Count(), "count")
      case f: SqlMinMaxAggFunction if !call.isDistinct && call.getArgList.size() == 1 =>
        val attrIndex: Int = project(call.getArgList.get(0))
        if (attrIndex == 0) None else {
          val binding = sft.getType(attrIndex - 1).getBinding
          if (isUnsupportedType(binding)) None else {
            val attrName = sft.getDescriptor(attrIndex - 1).getLocalName
            Some(Stat.MinMax(attrName), if (f.getKind == SqlKind.MIN) "min" else "max")
          }
        }
      // TODO: support more aggregation functions
      case _ => None
    }
  }

  private def resolveGroupedAttribute(groupedColumnId: Option[Int], sft: SimpleFeatureType, projection: Seq[Int]): Option[String] = {
    val project = (ordinal: Int) => if (projection.nonEmpty) projection(ordinal) else ordinal
    groupedColumnId match {
      case Some(columnId) =>
        val attrIndex = project(columnId)
        // Validate if grouped column was supported by GeoMesa
        if (attrIndex == 0) None else {
          val attrType = sft.getType(attrIndex - 1)
          val binding = attrType.getBinding
          if (isUnsupportedType(binding)) None
          else Some(sft.getDescriptor(attrIndex - 1).getLocalName)
        }
      case None => Some("")
    }
  }

  private def isUnsupportedType(binding: Class[_]): Boolean =
    binding == classOf[Geometry] ||
      binding == classOf[java.lang.Boolean] ||
      binding == classOf[Array[Byte]] ||
      binding == classOf[UUID]
}

object GeoMesaAggregateRule {
  val CONFIG = ImmutableGeoMesaRuleConfig.builder()
    .operandSupplier(b0 => b0.operand(classOf[LogicalAggregate])
      // GeoMesa only supports grouping by at most one attribute
      .predicate(agg => {
        val groupSet = agg.getGroupSet
        val cardinality = groupSet.cardinality()
        agg.getGroupType == Aggregate.Group.SIMPLE && cardinality <= 1
      })
      .oneInput(b1 => b1.operand(classOf[GeoMesaLogicalTableScan])
        .predicate(!_.isAggregatedOrLimited)
        .anyInputs()))
    .ruleFactory(new GeoMesaAggregateRule(_))
    .build()
}
