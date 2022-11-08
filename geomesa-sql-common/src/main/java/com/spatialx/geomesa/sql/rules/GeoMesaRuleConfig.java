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

package com.spatialx.geomesa.sql.rules;

import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.plan.RelOptRule;
import org.immutables.value.Value;

import java.util.function.Function;

/**
 * Generic Config class for defining query rewriting rules for geospatial
 * queries.
 */
@Value.Immutable
public interface GeoMesaRuleConfig extends Config {
  Function<GeoMesaRuleConfig, RelOptRule> ruleFactory();

  Config withRuleFactory(Function<GeoMesaRuleConfig, RelOptRule> factory);

  @Override default RelOptRule toRule() {
    return ruleFactory().apply(this);
  }
}
