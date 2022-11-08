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

package org.apache.calcite.adapter.enumerable

import com.google.common.collect.ImmutableList
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexBuilder, RexNode}

/**
 * ExtendedEnumUtils exposes some protected methods defined in [[org.apache.calcite.adapter.enumerable.EnumUtils]].
 * We can implement these methods ourselves but we're too lazy to do that.
 */
object ExtendedEnumUtils {
  def generatePredicate(implementor: EnumerableRelImplementor,
                        rexBuilder: RexBuilder,
                        left: RelNode,
                        right: RelNode,
                        leftPhysType: PhysType,
                        rightPhysType: PhysType,
                        condition: RexNode): Expression =
    EnumUtils.generatePredicate(implementor, rexBuilder, left, right, leftPhysType, rightPhysType, condition)

  def joinSelector(joinType: JoinRelType, physType: PhysType, leftPhysType: PhysType, rightPhysType: PhysType): Expression =
    EnumUtils.joinSelector(joinType, physType, ImmutableList.of(leftPhysType, rightPhysType))
}
