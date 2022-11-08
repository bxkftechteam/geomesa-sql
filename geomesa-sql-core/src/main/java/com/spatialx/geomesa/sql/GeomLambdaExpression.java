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

package com.spatialx.geomesa.sql;

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.locationtech.jts.geom.Geometry;

/**
 * Generating a lambda expression returning a geometry object.
 * This class is implemented in java for working around a scala compiler bug, which prevents us from of calling
 * java overloaded method with varargs from scala.
 */
public class GeomLambdaExpression {
    public static FunctionExpression<?> lambda(BlockStatement body, ParameterExpression parameter) {
      return Expressions.lambda(GeomFunction1.class, body, parameter);
    }

    public interface GeomFunction1<T0> extends Function1<T0, Geometry> {
        Geometry apply(T0 a0);
    }
}
