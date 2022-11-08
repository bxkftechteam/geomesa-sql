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

import org.opengis.feature.simple.SimpleFeature

/**
  * Converter implementation for converting simple features to array of
  * objects. This converter is for result set with more than one columns.
  */
class SimpleFeatureArrayConverter[T >: Array[AnyRef] <: AnyRef](val fieldIndices: Seq[Int]) extends SimpleFeatureConverter[T] {

  def this(fieldCount: Int) = this(Range(0, fieldCount))

  private val fieldIndicesArray = fieldIndices.toArray
  private val fieldCount = fieldIndicesArray.length

  override def convert(sf: SimpleFeature): T = {
    val row = new Array[AnyRef](fieldCount)
    var k = 0
    while (k < fieldCount) {
      val idx = fieldIndicesArray(k)
      val value = if (idx == 0) sf.getID else AttributeConverter.convert(sf.getAttribute(idx - 1))
      row(k) = value
      k += 1
    }
    row
  }
}
