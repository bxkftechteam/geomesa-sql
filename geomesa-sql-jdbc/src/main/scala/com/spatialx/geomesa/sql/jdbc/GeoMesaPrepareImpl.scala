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

package com.spatialx.geomesa.sql.jdbc

import org.apache.calcite.avatica.ColumnMetaData
import org.apache.calcite.jdbc.CalcitePrepare
import org.apache.calcite.plan.{Context, RelOptCostFactory, RelOptPlanner}
import org.apache.calcite.prepare.CalcitePrepareImpl
import org.apache.calcite.runtime.Bindable
import com.spatialx.geomesa.sql.rules.EnumerableSpatialJoinRule
import com.typesafe.scalalogging.LazyLogging
import java.lang.reflect.Type
import java.sql.Types
import java.util

/**
 * Adapted JDBC PrepareImpl to GeoMesa
 */
class GeoMesaPrepareImpl extends CalcitePrepareImpl with LazyLogging {
  override def createPlanner(prepareContext: CalcitePrepare.Context,
                             externalContext: Context,
                             costFactory: RelOptCostFactory): RelOptPlanner = {
    val planner = super.createPlanner(prepareContext, externalContext, costFactory)
    planner.addRule(EnumerableSpatialJoinRule.CONFIG.toRule)
    planner
  }

  /**
   * Rewrite the `CalciteSignature.columns` attribute, return `JAVA_OBJECT` instead of `VARCHAR` when meeting geometry field
   * original code: https://github.com/apache/calcite/blob/calcite-1.32.0/core/src/main/java/org/apache/calcite/prepare/CalcitePrepareImpl.java#L805
   */
  override def prepareSql[T](context: CalcitePrepare.Context,
                             query: CalcitePrepare.Query[T],
                             elementType: Type,
                             maxRowCount: Long): CalcitePrepare.CalciteSignature[T] = {
    val sign = super.prepareSql(context, query, elementType, maxRowCount)
    val newColumns = new util.ArrayList[ColumnMetaData](sign.columns.size)
    sign.columns.forEach((col: ColumnMetaData) => {
      if (col.`type`.getName == "GEOMETRY") {
        val newType = ColumnMetaData.scalar(Types.JAVA_OBJECT, col.`type`.getName, col.`type`.rep)
        newColumns.add(new ColumnMetaData(col.ordinal, col.autoIncrement, col.caseSensitive,
          col.searchable, col.currency, col.nullable, col.signed, col.displaySize, col.label,
          col.columnName, col.schemaName, col.precision, col.scale, col.tableName, col.catalogName,
          newType, col.readOnly, col.writable, col.definitelyWritable, newType.columnClassName))
      } else newColumns.add(col)
    })
    try {
      val bindableField = classOf[CalcitePrepare.CalciteSignature[_]].getDeclaredField("bindable")
      bindableField.setAccessible(true)
      val bindableVal = bindableField.get(sign).asInstanceOf[Bindable[T]]
      return new CalcitePrepare.CalciteSignature(sign.sql, sign.parameters, sign.internalParameters,
        sign.rowType, newColumns, sign.cursorFactory, sign.rootSchema, sign.getCollationList, maxRowCount,
        bindableVal, sign.statementType)
    } catch {
      case e@(_: IllegalAccessException | _: NoSuchFieldException) =>
        logger.warn("Failed to tamper with column metadata to return JTS Geometry objects for GEOMETRY values", e)
        sign
    }
  }
}
