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

import com.google.common.collect.BoundType
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.util.{NlsString, Sarg, TimestampString}
import org.geotools.factory.CommonFactoryFinder
import com.spatialx.geomesa.sql.enumerator.AttributeConverter.convertLocalTimestamp
import com.spatialx.geomesa.sql.rules.RexNodeTranslator.ff
import org.locationtech.geomesa.filter.orFilters
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Literal, PropertyName, Expression => GTExpression}
import org.opengis.filter.{FilterFactory2, Filter => GTFilter}

import java.util.{Calendar, Date, TimeZone}
import scala.util.Try

/**
  * Translating RexNode to GeoTools filter or expression
  */
class RexNodeTranslator(sft: SimpleFeatureType, projection: Array[Int]) {
  import scala.collection.JavaConverters._

  def this(sft: SimpleFeatureType, projection: Seq[Int]) = {
    this(sft, projection.toArray)
  }

  def rexNodeToGtFilter(predicate: RexNode): Option[GTFilter] = {
    predicate.getKind match {
      case SqlKind.OR | SqlKind.AND =>
        val operands = predicate.asInstanceOf[RexCall].getOperands.asScala
        val gtFilters = operands.flatMap(rexNodeToGtFilter)
        if (gtFilters.size != operands.size) None else {
          Some(if (predicate.getKind == SqlKind.OR) ff.or(gtFilters.asJava) else ff.and(gtFilters.asJava))
        }
      case SqlKind.NOT =>
        val operand = predicate.asInstanceOf[RexCall].getOperands.get(0)
        rexNodeToGtFilter(operand).map(ff.not)
      case SqlKind.EQUALS =>
        val operands = predicate.asInstanceOf[RexCall].getOperands
        val lhs = operands.get(0)
        val rhs = operands.get(1)
        if (lhs.getKind == SqlKind.INPUT_REF &&
          attributeIndexOf(lhs.asInstanceOf[RexInputRef]) == 0 &&
          rhs.getKind == SqlKind.LITERAL) {
          // __FID__ = RexLiteral
          val value = rhs.asInstanceOf[RexLiteral].getValueAs(classOf[String])
          Some(ff.id(ff.featureId(value)))
        } else (rexNodeToGtExpr(lhs), rexNodeToGtExpr(rhs)) match {
          case (Some(lhs), Some(rhs)) => Some(ff.equals(lhs, rhs))
          case _ => None
        }
      case SqlKind.IS_NULL =>
        val operand = predicate.asInstanceOf[RexCall].getOperands.get(0)
        rexNodeToGtExpr(operand).map(ff.isNull)
      case SqlKind.IS_NOT_NULL =>
        val operand = predicate.asInstanceOf[RexCall].getOperands.get(0)
        rexNodeToGtExpr(operand).map(expr => ff.not(ff.isNull(expr)))
      case kind if SqlKind.BINARY_COMPARISON.contains(kind) =>
        val operands = predicate.asInstanceOf[RexCall].getOperands
        (rexNodeToGtExpr(operands.get(0)), rexNodeToGtExpr(operands.get(1))) match {
          case (Some(lhs), Some(rhs)) => binaryComparisonToGtFilter(kind, lhs, rhs)
          case _ => None
        }
      case SqlKind.LIKE =>
        val operands = predicate.asInstanceOf[RexCall].getOperands
        val lhs = rexNodeToGtExpr(operands.get(0))
        val rhs = operands.get(1)
        (lhs, rhs.getKind, rhs.getType.getSqlTypeName) match {
          case (Some(l), SqlKind.LITERAL, SqlTypeName.CHAR) =>
            val pattern = rhs.asInstanceOf[RexLiteral].getValueAs(classOf[String])
            Some(ff.like(l, pattern, "%", "_", "\\"))
          case _ => None
        }
      case SqlKind.SEARCH =>
        val operands = predicate.asInstanceOf[RexCall].getOperands
        val lhs = operands.get(0)
        val bounds: RexNode = operands.get(1)
        if (bounds.getKind != SqlKind.LITERAL) None else {
          lhs match {
            case l: RexInputRef if attributeIndexOf(l) == 0 =>
              searchNodeToFeatureIdsFilter(bounds)
            case l => rexNodeToGtExpr(l) match {
              case Some(lhsExpr) => searchNodeToGtFilter(bounds, lhsExpr)
              case None => None
            }
          }
        }
      case _ =>
        predicate match {
          case rexCall: RexCall =>
            val operator = rexCall.getOperator
            val operands = rexCall.getOperands.asScala
            if (!operator.isInstanceOf[SqlUserDefinedFunction]) None else {
              val gtExprs = operands.flatMap(expr => rexNodeToGtExpr(expr))
              if (gtExprs.size == operands.size) udfToGtFilter(operator.asInstanceOf[SqlUserDefinedFunction], gtExprs)
              else None
            }
          case _ => None
        }
    }
  }

  def rexNodeToGtExpr(rexNode: RexNode): Option[GTExpression] = {
    rexNode.getKind match {
      case SqlKind.LITERAL =>
        val rexLiteral = rexNode.asInstanceOf[RexLiteral]
        rexLiteral.getValue match {
          case geom: Geometry => Some(ff.literal(geom))
          case value => rexLiteral.getTypeName match {
            case SqlTypeName.CHAR => Some(ff.literal(rexLiteral.getValueAs(classOf[String])))
            case SqlTypeName.TIME | SqlTypeName.DATE | SqlTypeName.TIMESTAMP =>
              val cal = rexLiteral.getValueAs(classOf[Calendar])
              val localTimestamp = cal.getTimeInMillis
              Some(ff.literal(convertLocalTimestamp(localTimestamp)))
            case _ => Some(ff.literal(value))
          }
        }
      case SqlKind.INPUT_REF =>
        val index = attributeIndexOf(rexNode.asInstanceOf[RexInputRef])
        if (index == 0) None else {
          val fieldName = sft.getDescriptor(index - 1).getName
          Some(ff.property(fieldName))
        }
      case SqlKind.CAST =>
        val rexCall = rexNode.asInstanceOf[RexCall]
        val operand = rexCall.getOperands.get(0)
        val castType = rexCall.getType
        rexNodeToGtExpr(operand) match {
          case Some(operandGtExpr: PropertyName) =>
            val propType = sft.getDescriptor(operandGtExpr.getPropertyName).getType
            if (propType.getBinding == classOf[java.lang.Float] && castType.getSqlTypeName == SqlTypeName.DOUBLE) {
              Some(operandGtExpr)
            } else None
          case None => None
        }
      case _ => None
    }
  }

  private def binaryComparisonToGtFilter(kind: SqlKind, expr1: GTExpression, expr2: GTExpression): Option[GTFilter] = {
    kind match {
      case SqlKind.EQUALS => Some(ff.equals(expr1, expr2))
      case SqlKind.LESS_THAN => Some(ff.less(expr1, expr2))
      case SqlKind.LESS_THAN_OR_EQUAL => Some(ff.lessOrEqual(expr1, expr2))
      case SqlKind.GREATER_THAN => Some(ff.greater(expr1, expr2))
      case SqlKind.GREATER_THAN_OR_EQUAL => Some(ff.greaterOrEqual(expr1, expr2))
      case SqlKind.NOT_EQUALS => Some(ff.notEqual(expr1, expr2))
      case _ => None
    }
  }

  private def udfToGtFilter(udf: SqlUserDefinedFunction, gtExprs: Seq[GTExpression]): Option[GTFilter] = {
    udf.getName match {
      case "ST_INTERSECTS" => Some(ff.intersects(gtExprs(0), gtExprs(1)))
      case "ST_EQUALS" => Some(ff.equals(gtExprs(0), gtExprs(1)))
      case "ST_CROSSES" => Some(ff.crosses(gtExprs(0), gtExprs(1)))
      case "ST_CONTAINS" => Some(ff.contains(gtExprs(0), gtExprs(1)))
      case "ST_OVERLAPS" => Some(ff.overlaps(gtExprs(0), gtExprs(1)))
      case "ST_TOUCHES" => Some(ff.touches(gtExprs(0), gtExprs(1)))
      case "ST_WITHIN" => Some(ff.within(gtExprs(0), gtExprs(1)))
      case "ST_DWITHIN" => dwithinGtFilter(gtExprs(0), gtExprs(1), gtExprs(2))
      case _ => None
    }
  }

  private def dwithinGtFilter(lhs: GTExpression, rhs: GTExpression, d: GTExpression): Option[GTFilter] = {
    d match {
      case l: Literal =>
        Try {
          val dist = l.getValue.asInstanceOf[Number].doubleValue()
          if (lhs.isInstanceOf[PropertyName])
            Some(ff.intersects(lhs, ff.function("buffer", rhs, ff.literal(dist))))
          else
            Some(ff.intersects(rhs, ff.function("buffer", lhs, ff.literal(dist))))
        }.getOrElse(None)
      case _ => None
    }
  }

  private def searchNodeToGtFilter(bounds: RexNode, lhs: GTExpression): Option[GTFilter] = {
    bounds.asInstanceOf[RexLiteral].getValue match {
      case sarg: Sarg[_] =>
        val rangeGtFilters = sarg.rangeSet.asRanges().asScala.map { range =>
          if (range.hasLowerBound && range.hasUpperBound &&
            range.lowerBoundType() == BoundType.CLOSED &&
            range.upperBoundType() == BoundType.CLOSED &&
            range.lowerEndpoint() == range.upperEndpoint()) {
            // lhs in [A, A], which is a point query
            ff.equals(lhs, ff.literal(rangeEndPointToGtLiteral(range.lowerEndpoint())))
          } else {
            val lowerFilter = {
              if (!range.hasLowerBound) None else {
                val lowerExpr = ff.literal(rangeEndPointToGtLiteral(range.lowerEndpoint()))
                if (range.lowerBoundType() == BoundType.OPEN) Some(ff.greater(lhs, lowerExpr))
                else Some(ff.greaterOrEqual(lhs, lowerExpr))
              }
            }
            val upperFilter = {
              if (!range.hasUpperBound) None else {
                val upperExpr = ff.literal(rangeEndPointToGtLiteral(range.upperEndpoint()))
                if (range.upperBoundType() == BoundType.OPEN) Some(ff.less(lhs, upperExpr))
                else Some(ff.lessOrEqual(lhs, upperExpr))
              }
            }
            (lowerFilter, upperFilter) match {
              case (Some(lower), Some(upper)) => ff.and(lower, upper)
              case (Some(lower), None) => lower
              case (None, Some(upper)) => upper
              case _ => throw new RuntimeException("SEARCH range is not bounded")
            }
          }
        }
        Some(orFilters(rangeGtFilters.toSeq))
      case _ => None
    }
  }

  private def searchNodeToFeatureIdsFilter(bounds: RexNode): Option[GTFilter] = {
    bounds.asInstanceOf[RexLiteral].getValue match {
      case sarg: Sarg[_] =>
        val ranges = sarg.rangeSet.asRanges()
        val ids = ranges.asScala.flatMap { range =>
          if (range.hasLowerBound && range.hasUpperBound &&
            range.lowerBoundType() == BoundType.CLOSED &&
            range.upperBoundType() == BoundType.CLOSED &&
            range.lowerEndpoint() == range.upperEndpoint()) {
            // lhs in [A, A], which is a point query
            range.lowerEndpoint() match {
              case v: NlsString => Some(ff.featureId(v.getValue))
              case _ => None
            }
          } else None
        }
        if (ids.size == ranges.size()) Some(ff.id(ids.toSet.asJava)) else None
      case _ => None
    }
  }

  private def rangeEndPointToGtLiteral[C <: Comparable[_]](value: C): Any = {
    value match {
      case ts: TimestampString => convertLocalTimestamp(ts.getMillisSinceEpoch)
      case str: NlsString => str.getValue
      case _ => value
    }
  }

  private def attributeIndexOf(rexNode: RexInputRef): Int = {
    val inputRef = rexNode.getIndex
    if (projection.nonEmpty) projection(inputRef) else inputRef
  }
}

object RexNodeTranslator {
  val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2
}
