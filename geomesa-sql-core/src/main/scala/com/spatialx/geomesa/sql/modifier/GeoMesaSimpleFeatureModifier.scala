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

package com.spatialx.geomesa.sql.modifier

import org.opengis.feature.simple.SimpleFeature

/**
 * Modifier for GeoMesa simple feature. Implementation of this trait may batch modifications
 * for better throughput.
 */
trait GeoMesaSimpleFeatureModifier {
  /**
   * Modify a simple feature. The modification may not take place until flush is called
   * @param sf simple feature to modify
   */
  def modify(sf: SimpleFeature): Unit

  /**
   * Flush modifications
   * @return Number of actual modifications flushed
   */
  def flush(): Long

  /**
   * Close the modifier, should call flush before calling close, otherwise batched modifications
   * might get lost.
   */
  def close(): Unit
}
