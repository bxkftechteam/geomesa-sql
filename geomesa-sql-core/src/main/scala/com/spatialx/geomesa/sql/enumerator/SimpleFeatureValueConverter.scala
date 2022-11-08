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
  * values. This converter is for result set with a single column.
  */
class SimpleFeatureValueConverter(val fieldIndex: Int) extends SimpleFeatureConverter[AnyRef] {

  override def convert(sf: SimpleFeature): AnyRef = {
    if (fieldIndex == 0) sf.getID else {
      AttributeConverter.convert(sf.getAttribute(fieldIndex - 1))
    }
  }
}
