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

import org.apache.calcite
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName
import org.geotools.data.DataStore
import org.locationtech.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import java.{util => ju}

/**
  * Base class for Table interface implementation for adapting GeoMesa
  * DataStores to Calcite.
  */
abstract class GeoMesaTable(val ds: DataStore, val typeName: String) extends AbstractTable {
  /**
    * Populate row type of this table from simple feature type, with an extra
    * __FID__ field for feature IDs.
    */
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    Option(getSimpleFeatureType) match {
      case Some(sft) => GeoMesaTable.simpleFeatureTypeToRowType(sft, typeFactory)
      case None => throw new RuntimeException(s"cannot get schema by name $typeName")
    }
  }

  /**
    * Populate field types of rows for easier implementing Table interface
    */
  protected def getFieldTypes(typeFactory: RelDataTypeFactory): Seq[(String, RelDataType)] = {
    Option(getSimpleFeatureType) match {
      case Some(sft) => GeoMesaTable.simpleFeatureTypeToFieldTypes(sft, typeFactory)
      case None => throw new RuntimeException(s"cannot get schema by name $typeName")
    }
  }

  /**
    * Get simple feature type of tis GeoMesa table
    */
  def getSimpleFeatureType: SimpleFeatureType = sft

  private lazy val sft = ds.getSchema(typeName)
}

object GeoMesaTable {
  import scala.collection.JavaConverters._

  def simpleFeatureTypeToRowType(sft: SimpleFeatureType, typeFactory: RelDataTypeFactory): RelDataType = {
    val fieldTypes = simpleFeatureTypeToFieldTypes(sft, typeFactory)
    val fieldEntries = fieldTypes.map { case (name, fieldType) => calcite.util.Pair.of(name, fieldType) }
    typeFactory.createStructType(fieldEntries.asJava)
  }

  def simpleFeatureTypeToFieldTypes(sft: SimpleFeatureType, typeFactory: RelDataTypeFactory): Seq[(String, RelDataType)] = {
    val attributeDescriptors = sft.getAttributeDescriptors.asScala
    val structFields = attributeDescriptors.map(attrDesc => attributeDescriptorToStructField(attrDesc, typeFactory))
    fidRelDataType(typeFactory) +: structFields
  }

  private def attributeDescriptorToStructField(attributeDescriptor: AttributeDescriptor, typeFactory: RelDataTypeFactory)
      : (String, RelDataType) = {
    val name = attributeDescriptor.getLocalName
    val fieldType = attributeDescriptor.getType.getBinding match {
      case c if c == classOf[java.lang.String]    => toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR)
      case c if c == classOf[java.lang.Integer]   => toNullableRelDataType(typeFactory, SqlTypeName.INTEGER)
      case c if c == classOf[java.lang.Double]    => toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE)
      case c if c == classOf[java.lang.Long]      => toNullableRelDataType(typeFactory, SqlTypeName.BIGINT)
      case c if c == classOf[java.lang.Float]     => toNullableRelDataType(typeFactory, SqlTypeName.REAL)
      case c if c == classOf[java.lang.Boolean]   => toNullableRelDataType(typeFactory, SqlTypeName.BOOLEAN)
      case c if c == classOf[ju.UUID]             => toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR)
      case c if c == classOf[ju.Date]             => toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP)
      case c if c == classOf[java.sql.Date]       => toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP)
      case c if c == classOf[java.sql.Timestamp]  => toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP)
      case c if c == classOf[Geometry]            => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[Point]               => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[LineString]          => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[Polygon]             => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[MultiPoint]          => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[MultiLineString]     => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[MultiPolygon]        => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[GeometryCollection]  => toNullableRelDataType(typeFactory, SqlTypeName.GEOMETRY)
      case c if c == classOf[Array[Byte]]         => toNullableRelDataType(typeFactory, SqlTypeName.VARBINARY)
      case c => throw new NotImplementedError(s"simple feature attribute type $c was not supported")
    }
    (name, fieldType)
  }

  private def fidRelDataType(typeFactory: RelDataTypeFactory): (String, RelDataType) =
    (FID_FIELD_NAME, toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR))

  private def toNullableRelDataType(typeFactory: RelDataTypeFactory, sqlTypeName: SqlTypeName): RelDataType = {
    typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true)
  }
}
