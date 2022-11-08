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

package com.spatialx.geomesa.sql

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.{Enumerable, Enumerator, QueryProvider, Queryable}
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.plan.{RelOptCluster, RelOptTable}
import org.apache.calcite.prepare.Prepare
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.impl.AbstractTableQueryable
import org.apache.calcite.schema._
import org.geotools.data.DataStore
import com.spatialx.geomesa.sql.enumerator.SimpleFeatureEnumerable
import com.spatialx.geomesa.sql.nodes.{GeoMesaLogicalTableModify, GeoMesaLogicalTableScan}

import java.lang.reflect.Type
import java.util
import java.util.concurrent.atomic.AtomicBoolean

/**
  * GeoMesa table implementation with query optimization rules. It rewrites the
  * query to push down predicates, projections and aggregations to geotools filters.
  */
class GeoMesaTranslatableTable(ds: DataStore, typeName: String, val params: Map[String, String])
  extends GeoMesaTable(ds, typeName)
    with QueryableTable with TranslatableTable with ModifiableTable {

  /**
    * Run geotools query on this GeoMesa schema
    */
  def query(root: DataContext, queryParams: GeoMesaQueryParams): Enumerable[AnyRef] = {
    val cancelFlag = Option(root) match {
      case Some(_) => DataContext.Variable.CANCEL_FLAG.get[AtomicBoolean](root)
      // called by GeoMesaQueryable.enumerator, no DataContext available
      case None => new AtomicBoolean
    }
    new SimpleFeatureEnumerable(ds, getSimpleFeatureType, cancelFlag, queryParams)
  }

  override def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] =
    new GeoMesaTranslatableTable.GeoMesaQueryable(queryProvider, schema, this, tableName)

  override def getElementType: Type = classOf[Array[AnyRef]]

  override def getExpression(schema: SchemaPlus, tableName: String, clazz: Class[_]): Expression =
    Schemas.tableExpression(schema, getElementType(), tableName, clazz)

  override def toRel(context: ToRelContext, relOptTable: RelOptTable): RelNode = {
    val cluster = context.getCluster
    // We emit a GeoMesaLogicalTableScan rel node, so that the entire tree is pure logical initially.
    // All logical transformations will be applied in pure logical convention.
    // GeoMesaTableLogicalToPhysicalRule will kick in and convert it to a GeoMesaTablePhysicalScan node in the
    // final physical tree.
    //
    // Ensuring the initial tree to be pure logical help us getting rid of weired bugs such as
    // https://issues.apache.org/jira/browse/CALCITE-4029?jql=project%20%3D%20CALCITE%20AND%20text%20~%20%22ProjectRemoveRule%22
    //
    // Most of calcite's builtin adapters don't do this, and they are all problematic :-(
    GeoMesaLogicalTableScan.create(cluster, relOptTable, context.getTableHints)
  }

  // This method is only used by EnumerableTableModify, and we'll roll our own physical node for GeoMesa table
  // modification, so we just provide a dummy implementation for this method.
  override def getModifiableCollection: util.Collection[_] = null

  // We emit GeoMesaLogicalTableModify, which will be converted to GeoMesaPhysicalTableModify by a converter rule.
  override def toModificationRel(cluster: RelOptCluster, table: RelOptTable, catalogReader: Prepare.CatalogReader,
                                 child: RelNode, operation: TableModify.Operation, updateColumnList: util.List[String],
                                 sourceExpressionList: util.List[RexNode], flattened: Boolean): TableModify = {
    GeoMesaLogicalTableModify.create(table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened)
  }
}

object GeoMesaTranslatableTable {
  /**
    * Implementation of Queryable based on GeoMesaTranslatableTable
    */
  class GeoMesaQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, table: GeoMesaTranslatableTable, tableName: String)
      extends AbstractTableQueryable[T](queryProvider, schema, table, tableName) {

    override def enumerator(): Enumerator[T] = {
      val enumerable = table.query(null, GeoMesaQueryParams(tableName)).asInstanceOf[Enumerable[T]]
      enumerable.enumerator()
    }

    /**
      * Called via code-generation
      */
    def query(root: DataContext, queryParams: GeoMesaQueryParams): Enumerable[AnyRef] =
      table.query(root, queryParams)
  }
}
