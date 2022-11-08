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

package com.spatialx.geomesa

package object sql {
  /**
    * Column/field name of feature ID. We need to do lots of special treatments
    * for feature ID columns since the SimpleFeature methods for accessing
    * feature ID are quite different from those for accessing other attributes.
    */
  val FID_FIELD_NAME = "__FID__"
}
