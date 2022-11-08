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

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import com.spatialx.geomesa.sql.nodes.{GeoMesaRel, GeoMesaToEnumerableConverter}

/**
  * Rule to convert a relational expression from GeoMesaRel.CONVENTION to EnumerableConvention
  */
class GeoMesaToEnumerableConverterRule(config: Config) extends ConverterRule(config) {
  def convert(rel: RelNode): RelNode = {
    val newTraitSet = rel.getTraitSet.replace(getOutConvention)
    new GeoMesaToEnumerableConverter(rel.getCluster, newTraitSet, rel)
  }
}

object GeoMesaToEnumerableConverterRule {
  val DEFAULT_CONFIG = Config.INSTANCE
    .withConversion(classOf[RelNode], GeoMesaRel.CONVENTION, EnumerableConvention.INSTANCE, "GeoMesaToEnumerableConverterRule")
    .withRuleFactory(new GeoMesaToEnumerableConverterRule(_))
}
