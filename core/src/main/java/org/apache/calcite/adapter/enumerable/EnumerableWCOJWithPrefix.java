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
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * A WCOJ operator that uses precomputed shared prefix bindings.
 *
 * <p>Created by {@link EnumerableCombineWCOJPrefixRule} when multiple WCOJ
 * operators within a Combine share a variable prefix. Instead of computing
 * the prefix independently, this operator receives prefix bindings from a
 * shared computation and continues with the suffix variables.
 *
 * <p>During code generation, operators in the same group (identified by
 * {@code groupId}) share a single prefix computation expression.
 */
@Experimental
public class EnumerableWCOJWithPrefix extends AbstractRelNode
    implements EnumerableRel {

  private final EnumerableWCOJ delegate;
  private final int prefixDepth;
  private final int groupId;

  protected EnumerableWCOJWithPrefix(
      EnumerableWCOJ delegate,
      int prefixDepth,
      int groupId) {
    super(delegate.getCluster(), delegate.getTraitSet());
    this.delegate = delegate;
    this.prefixDepth = prefixDepth;
    this.groupId = groupId;
    this.rowType = delegate.getRowType();
  }

  public static EnumerableWCOJWithPrefix create(
      EnumerableWCOJ delegate,
      int prefixDepth,
      int groupId) {
    return new EnumerableWCOJWithPrefix(delegate, prefixDepth, groupId);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    EnumerableWCOJ newDelegate =
        (EnumerableWCOJ) delegate.copy(traitSet, inputs);
    return new EnumerableWCOJWithPrefix(newDelegate, prefixDepth, groupId);
  }

  @Override public List<RelNode> getInputs() {
    return delegate.getInputs();
  }

  @Override public void replaceInput(int ordinalInParent, RelNode p) {
    delegate.replaceInput(ordinalInParent, p);
    recomputeDigest();
  }

  public EnumerableWCOJ getDelegate() {
    return delegate;
  }

  public int getPrefixDepth() {
    return prefixDepth;
  }

  public int getGroupId() {
    return groupId;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    delegate.explainTerms(pw);
    pw.item("prefixDepth", prefixDepth);
    pw.item("groupId", groupId);
    return pw;
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Cheaper than standalone WCOJ because prefix is shared
    RelOptCost baseCost = delegate.computeSelfCost(planner, mq);
    if (baseCost == null || baseCost.isInfinite()) {
      return baseCost;
    }
    // Reduce cost to reflect shared prefix work
    double reduction = 0.8; // 20% savings from shared prefix
    return planner.getCostFactory().makeCost(
        baseCost.getRows() * reduction, 0, 0);
  }

  @Override public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();

    // Visit all input children (same as EnumerableWCOJ)
    final List<Expression> inputExpressions = new ArrayList<>();
    final List<PhysType> inputPhysTypes = new ArrayList<>();

    for (Ord<RelNode> ord : Ord.zip(delegate.getInputs())) {
      final Result inputResult = implementor.visitChild(this, ord.i,
          (EnumerableRel) ord.e, pref);
      inputPhysTypes.add(inputResult.physType);

      Expression inputExpr = builder.append("input" + ord.i, inputResult.block);
      if (inputResult.physType.getFormat() != JavaRowFormat.ARRAY) {
        inputExpr = inputResult.physType.convertTo(inputExpr, JavaRowFormat.ARRAY);
      }
      inputExpressions.add(inputExpr);
    }

    final PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(), getRowType(), pref.preferArray());

    // Build inputs list
    final Expression inputsList = Expressions.call(
        java.util.Arrays.class, "asList",
        Expressions.newArrayInit(
            org.apache.calcite.linq4j.Enumerable.class,
            inputExpressions));

    // Build join key indices
    final List<Expression> joinKeyIndicesExprs = new ArrayList<>();
    for (int i = 0; i < delegate.getInputs().size(); i++) {
      final List<Integer> keysForInput = new ArrayList<>();
      for (EnumerableWCOJ.JoinVariable var : delegate.getVariables()) {
        for (int j = 0; j < var.occurrences.size(); j++) {
          if (var.occurrences.get(j).left == i) {
            keysForInput.add(var.occurrences.get(j).right);
          }
        }
      }
      int[] keyArray = keysForInput.stream().mapToInt(Integer::intValue).toArray();
      joinKeyIndicesExprs.add(Expressions.constant(keyArray));
    }
    final Expression joinKeyIndicesList = Expressions.call(
        java.util.Arrays.class, "asList",
        joinKeyIndicesExprs.toArray(new Expression[0]));

    // Build variable to inputs mapping
    final List<Expression> variableToInputsExprs = new ArrayList<>();
    for (EnumerableWCOJ.JoinVariable var : delegate.getVariables()) {
      int[] mapping = new int[var.occurrences.size() * 2];
      for (int j = 0; j < var.occurrences.size(); j++) {
        mapping[j * 2] = var.occurrences.get(j).left;
        mapping[j * 2 + 1] = var.occurrences.get(j).right;
      }
      variableToInputsExprs.add(Expressions.constant(mapping));
    }
    final Expression variableToInputsArray = Expressions.newArrayInit(
        int.class, 2, variableToInputsExprs);

    // Build result selector (same as EnumerableWCOJ)
    final ParameterExpression inputRows_ = Expressions.parameter(
        Object[][].class, "inputRows");
    final Expression resultSelector = buildResultSelector(
        implementor, physType, inputPhysTypes, inputRows_);

    // Build the prefix variable-to-inputs mapping
    // (only the first prefixDepth variables)
    final Expression trieCacheExpr = implementor.getTrieCacheExpr();
    final Expression trieCacheArg = trieCacheExpr != null ? trieCacheExpr
        : Expressions.constant(null,
            org.apache.calcite.linq4j.TrieCache.class);

    final List<Expression> prefixVarExprs = new ArrayList<>();
    final List<EnumerableWCOJ.JoinVariable> vars = delegate.getVariables();
    for (int v = 0; v < prefixDepth && v < vars.size(); v++) {
      EnumerableWCOJ.JoinVariable var = vars.get(v);
      int[] mapping = new int[var.occurrences.size() * 2];
      for (int j = 0; j < var.occurrences.size(); j++) {
        mapping[j * 2] = var.occurrences.get(j).left;
        mapping[j * 2 + 1] = var.occurrences.get(j).right;
      }
      prefixVarExprs.add(Expressions.constant(mapping));
    }
    final Expression prefixVarArray = Expressions.newArrayInit(
        int.class, 2, prefixVarExprs);

    // Each WCOJ with prefix generates its own prefix computation.
    // Trie building is shared via TrieCache; only the prefix backtracking
    // is duplicated across group members (cheap relative to trie construction).
    final Expression prefixBindingsExpr = Expressions.call(
        BuiltInMethod.WCOJ_PREFIX.method,
        inputsList,
        prefixVarArray,
        trieCacheArg,
        Expressions.constant(prefixDepth));

    // Generate call to wcojWithSharedPrefix
    final Expression wcojCall = Expressions.call(
        BuiltInMethod.WCOJ_WITH_SHARED_PREFIX.method,
        inputsList,
        joinKeyIndicesList,
        variableToInputsArray,
        resultSelector,
        trieCacheArg,
        prefixBindingsExpr,
        Expressions.constant(prefixDepth));

    return implementor.result(physType, builder.append(wcojCall).toBlock());
  }

  /**
   * Builds the result selector lambda (same logic as EnumerableWCOJ).
   */
  private Expression buildResultSelector(
      EnumerableRelImplementor implementor,
      PhysType physType,
      List<PhysType> inputPhysTypes,
      ParameterExpression inputRows_) {

    final org.apache.calcite.linq4j.tree.BlockBuilder lambdaBuilder =
        new org.apache.calcite.linq4j.tree.BlockBuilder();
    final List<Expression> outputFields = new ArrayList<>();
    final RelDataType rowType = getRowType();

    int outputFieldOffset = 0;
    for (int inputIdx = 0; inputIdx < delegate.getInputs().size(); inputIdx++) {
      final PhysType inputPhysType = inputPhysTypes.get(inputIdx);
      final int inputFieldCount = inputPhysType.getRowType().getFieldCount();

      final Expression inputRow = Expressions.arrayIndex(inputRows_,
          Expressions.constant(inputIdx));

      for (int fieldIdx = 0; fieldIdx < inputFieldCount; fieldIdx++) {
        final Expression field = Expressions.arrayIndex(
            Expressions.convert_(inputRow, Object[].class),
            Expressions.constant(fieldIdx));

        final RelDataType fieldType = rowType.getFieldList()
            .get(outputFieldOffset + fieldIdx).getType();
        final Type javaType = implementor.getTypeFactory().getJavaClass(fieldType);

        outputFields.add(EnumUtils.convert(field, javaType));
      }
      outputFieldOffset += inputFieldCount;
    }

    final Expression outputRow = physType.record(outputFields);
    lambdaBuilder.add(Expressions.return_(null, outputRow));

    return Expressions.lambda(lambdaBuilder.toBlock(), inputRows_);
  }
}
