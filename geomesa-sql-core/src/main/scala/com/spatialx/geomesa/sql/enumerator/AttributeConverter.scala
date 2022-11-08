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

package com.spatialx.geomesa.sql.enumerator

import org.apache.calcite.avatica.util.ByteString

import java.util.Date
import java.sql.Timestamp
import java.sql
import java.util.TimeZone
import java.util.UUID
import org.locationtech.jts.geom.Geometry

/**
  * Convert simple feature attribute to calcite object
  */
object AttributeConverter {
  /**
    * Convert simple feature attribute value to calcite SQL value
    * @param value simple feature attribute value
    * @return calcite SQL value
    */
  def convert(value: AnyRef): AnyRef = {
    value match {
      case v: org.locationtech.jts.geom.Geometry => v
      case v: Date => convertTime(v.getTime)
      case v: Timestamp => convertTime(v.getTime)
      case v: sql.Date => convertTime(v.getTime)
      case v: UUID => v.toString
      case _ => value
    }
  }

  /**
   * Convert calcite SQL value to simple feature attribute value
   * @param binding simple feature attribute binding
   * @param value calcite SQL value
   * @return simple feature attribute value
   */
  def convert(binding: Class[_], value: AnyRef): AnyRef = {
    binding match {
      case c if classOf[org.locationtech.jts.geom.Geometry].isAssignableFrom(c) => value.asInstanceOf[Geometry]
      case c if c == classOf[java.util.Date]
        || c == classOf[java.sql.Date]
        || c == classOf[java.sql.Timestamp] => convertLocalTimestamp(value.asInstanceOf[Long])
      case c if c == classOf[Array[Byte]] => value match {
        case byteString: ByteString => byteString.getBytes
        case _ => value
      }
      case _ => value
    }
  }

  private val LOCAL_TZ = TimeZone.getDefault

  /**
   * Convert local timestamp to Date object, where Date object is constructed from UTC timestamp
   * @param localTimestamp local timestamp
   * @return Date object constructed from UTC timestamp
   */
  def convertLocalTimestamp(localTimestamp: Long): Date = {
    val utcTimestamp = localTimestamp - LOCAL_TZ.getOffset(localTimestamp)
    new Date(utcTimestamp)
  }

  /**
    * Time values retrieved from GeoMesa are always UTC, so we need to add an
    * offset to convert it to local timestamp.
    */
  private def convertTime(timestamp: Long): java.lang.Long = {
    LOCAL_TZ.getOffset(timestamp) + timestamp
  }
}
