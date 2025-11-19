/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Combine} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableCombine extends Combine implements EnumerableRel {
  public EnumerableCombine(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelNode> inputs) {
    super(cluster, traitSet, inputs);
  }

  @Override public EnumerableCombine copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableCombine(getCluster(), traitSet, inputs);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final List<Expression> fieldExpressions = new ArrayList<>();

    // Implement each input and collect their results
    // Convert each Enumerable to a List since the row type is STRUCT<QUERY_0: ARRAY<...>, ...>
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      EnumerableRel input = (EnumerableRel) ord.e;
      final Result result = implementor.visitChild(this, ord.i, input, pref);
      Expression childExp =
          builder.append(
              "child" + ord.i,
              result.block);

      // Convert Enumerable to List for the array field
      // Each field in the struct is an ARRAY type, which maps to List in Java
      Expression listExp =
          builder.append(
              "list" + ord.i,
              Expressions.call(
                  childExp,
                  Types.lookupMethod(
                      org.apache.calcite.linq4j.Enumerable.class,
                      "toList")));
      fieldExpressions.add(listExp);
    }

    // The physical type represents the struct of all query results
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            rowType,
            pref.prefer(JavaRowFormat.CUSTOM));

    // Create the struct record with the list fields
    final Expression recordExpression =
        physType.record(fieldExpressions);

    // Return a singleton enumerable containing the struct as the single row
    // The Combine operator produces exactly one row containing all the child results
    final Expression singletonExpression =
        Expressions.call(
            Types.lookupMethod(
                org.apache.calcite.linq4j.Linq4j.class,
                "singletonEnumerable",
                Object.class),
            recordExpression);

    builder.add(singletonExpression);

    return implementor.result(physType, builder.toBlock());
  }
}
