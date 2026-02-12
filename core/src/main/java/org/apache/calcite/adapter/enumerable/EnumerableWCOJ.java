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
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a multi-way join using Worst-Case Optimal Join (WCOJ) algorithm.
 *
 * <p>WCOJ provides runtime guarantees proportional to worst-case output size,
 * particularly beneficial for cyclic queries (e.g., triangle queries) that
 * produce large intermediate results with traditional binary joins.
 *
 * <p>This RelNode accepts N inputs (not limited to 2) and only supports INNER joins.
 */
@Experimental
public class EnumerableWCOJ extends AbstractRelNode implements EnumerableRel {

  private final List<RelNode> inputs;
  private final RexNode joinCondition;
  private final List<JoinVariable> variables;

  /**
   * Creates an EnumerableWCOJ.
   *
   * @param cluster the cluster
   * @param traitSet the trait set
   * @param inputs the input relations (N inputs for N-way join)
   * @param joinCondition the combined equi-join predicate
   * @param variables the global variable ordering for WCOJ
   * @param rowType the output row type
   */
  protected EnumerableWCOJ(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode joinCondition,
      List<JoinVariable> variables,
      RelDataType rowType) {
    super(cluster, traitSet);
    // Use a mutable list so replaceInput can work
    this.inputs = new ArrayList<>(inputs);
    this.joinCondition = joinCondition;
    this.variables = ImmutableList.copyOf(variables);
    this.rowType = rowType;
  }

  /**
   * Creates an EnumerableWCOJ.
   */
  public static EnumerableWCOJ create(
      List<RelNode> inputs,
      RexNode joinCondition,
      List<JoinVariable> variables,
      RelDataType rowType) {
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
    return new EnumerableWCOJ(cluster, traitSet, inputs, joinCondition, variables, rowType);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableWCOJ(getCluster(), traitSet, inputs, joinCondition, variables, rowType);
  }

  @Override public List<RelNode> getInputs() {
    return inputs;
  }

  @Override public void replaceInput(int ordinalInParent, RelNode p) {
    inputs.set(ordinalInParent, p);
    recomputeDigest();
  }

  public RexNode getJoinCondition() {
    return joinCondition;
  }

  public List<JoinVariable> getVariables() {
    return variables;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    pw.item("joinCondition", joinCondition);
    pw.item("variables", variables);
    return pw;
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Cost model for WCOJ:
    // - Build cost: sum of input sizes (for building tries)
    // - Probe cost: bounded by AGM (worst-case output size)
    // For cyclic queries, WCOJ should be cheaper than binary join chains
    double buildCost = 0;
    for (RelNode input : inputs) {
      double inputRows = mq.getRowCount(input);
      if (Double.isInfinite(inputRows)) {
        return planner.getCostFactory().makeInfiniteCost();
      }
      buildCost += inputRows;
    }

    double outputRows = mq.getRowCount(this);
    if (Double.isInfinite(outputRows)) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    // Total cost = build all tries + produce output
    double totalCost = buildCost + outputRows;
    return planner.getCostFactory().makeCost(totalCost, 0, 0);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();

    // Visit all input children and build expressions
    // WCOJ requires inputs as Object[] arrays, so we convert to ARRAY format
    final List<Expression> inputExpressions = new ArrayList<>();
    final List<PhysType> inputPhysTypes = new ArrayList<>();

    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      final Result inputResult = implementor.visitChild(this, ord.i,
          (EnumerableRel) ord.e, pref);
      inputPhysTypes.add(inputResult.physType);

      // Convert input to Object[] format if not already
      Expression inputExpr = builder.append("input" + ord.i, inputResult.block);
      if (inputResult.physType.getFormat() != JavaRowFormat.ARRAY) {
        // Convert to array format
        inputExpr = inputResult.physType.convertTo(inputExpr, JavaRowFormat.ARRAY);
      }
      inputExpressions.add(inputExpr);
    }

    // Build the PhysType for the output
    final PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(), getRowType(), pref.preferArray());

    // Build the list of input enumerables (List<Enumerable<Object[]>>)
    final Expression inputsList = Expressions.call(
        java.util.Arrays.class, "asList",
        Expressions.newArrayInit(
            org.apache.calcite.linq4j.Enumerable.class,
            inputExpressions));

    // Build join key indices for each input (List<int[]>)
    final List<Expression> joinKeyIndicesExprs = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      final List<Integer> keysForInput = new ArrayList<>();
      for (JoinVariable var : variables) {
        for (int j = 0; j < var.occurrences.size(); j++) {
          if (var.occurrences.get(j).left == i) {
            keysForInput.add(var.occurrences.get(j).right);
          }
        }
      }
      int[] keyArray = keysForInput.stream().mapToInt(Integer::intValue).toArray();
      joinKeyIndicesExprs.add(Expressions.constant(keyArray));
    }
    // Use varargs form of Arrays.asList to get List<int[]>
    final Expression joinKeyIndicesList = Expressions.call(
        java.util.Arrays.class, "asList",
        joinKeyIndicesExprs.toArray(new Expression[0]));

    // Build variable to inputs mapping (int[][])
    // Each row is [inputIdx0, fieldIdx0, inputIdx1, fieldIdx1, ...]
    final List<Expression> variableToInputsExprs = new ArrayList<>();
    for (JoinVariable var : variables) {
      int[] mapping = new int[var.occurrences.size() * 2];
      for (int j = 0; j < var.occurrences.size(); j++) {
        mapping[j * 2] = var.occurrences.get(j).left;
        mapping[j * 2 + 1] = var.occurrences.get(j).right;
      }
      variableToInputsExprs.add(Expressions.constant(mapping));
    }
    final Expression variableToInputsArray = Expressions.newArrayInit(
        int.class, 2, variableToInputsExprs);

    // Build result selector
    // Takes Object[][] (one row per input) and produces the output row
    final ParameterExpression inputRows_ = Expressions.parameter(
        Object[][].class, "inputRows");
    final Expression resultSelector = buildResultSelector(
        implementor, physType, inputPhysTypes, inputRows_);

    // Generate the call to EnumerableDefaults.wcoj.
    // If a shared TrieCache is available (from a parent Combine), use the
    // cache-aware overload so tries can be reused across WCOJ operators.
    final Expression trieCacheExpr = implementor.getTrieCacheExpr();
    final Expression wcojCall;
    if (trieCacheExpr != null) {
      wcojCall = Expressions.call(
          BuiltInMethod.WCOJ_WITH_CACHE.method,
          inputsList,
          joinKeyIndicesList,
          variableToInputsArray,
          resultSelector,
          trieCacheExpr);
    } else {
      wcojCall = Expressions.call(
          BuiltInMethod.WCOJ.method,
          inputsList,
          joinKeyIndicesList,
          variableToInputsArray,
          resultSelector);
    }

    return implementor.result(physType, builder.append(wcojCall).toBlock());
  }

  /**
   * Builds the result selector lambda that takes matched rows from each input
   * and produces the output row.
   */
  private Expression buildResultSelector(
      EnumerableRelImplementor implementor,
      PhysType physType,
      List<PhysType> inputPhysTypes,
      ParameterExpression inputRows_) {

    final BlockBuilder lambdaBuilder = new BlockBuilder();
    final List<Expression> outputFields = new ArrayList<>();

    int outputFieldOffset = 0;
    for (int inputIdx = 0; inputIdx < inputs.size(); inputIdx++) {
      final PhysType inputPhysType = inputPhysTypes.get(inputIdx);
      final int inputFieldCount = inputPhysType.getRowType().getFieldCount();

      // Get the row for this input: inputRows[inputIdx]
      final Expression inputRow = Expressions.arrayIndex(inputRows_,
          Expressions.constant(inputIdx));

      // Extract each field from the input row
      for (int fieldIdx = 0; fieldIdx < inputFieldCount; fieldIdx++) {
        // Cast the row to Object[] and access the field
        final Expression field = Expressions.arrayIndex(
            Expressions.convert_(inputRow, Object[].class),
            Expressions.constant(fieldIdx));

        // Get the expected type for this output field
        final RelDataType fieldType = rowType.getFieldList()
            .get(outputFieldOffset + fieldIdx).getType();
        final Type javaType = implementor.getTypeFactory().getJavaClass(fieldType);

        // Convert to the expected type
        outputFields.add(EnumUtils.convert(field, javaType));
      }
      outputFieldOffset += inputFieldCount;
    }

    // Build the output row using the physType's record method
    final Expression outputRow = physType.record(outputFields);
    lambdaBuilder.add(Expressions.return_(null, outputRow));

    return Expressions.lambda(lambdaBuilder.toBlock(), inputRows_);
  }

  /**
   * Represents a join variable shared across relations.
   *
   * <p>A join variable corresponds to a column that is equated across multiple
   * relations via the join condition. For example, in a triangle query:
   * <pre>
   *   SELECT * FROM R, S, T WHERE R.a = S.a AND S.b = T.b AND T.c = R.c
   * </pre>
   *
   * <p>There are three variables:
   * <ul>
   *   <li>Variable 0: occurs at (R, a) and (S, a)</li>
   *   <li>Variable 1: occurs at (S, b) and (T, b)</li>
   *   <li>Variable 2: occurs at (T, c) and (R, c)</li>
   * </ul>
   */
  public static class JoinVariable {
    /** Unique identifier for this variable. */
    public final int variableId;

    /**
     * List of (inputIndex, fieldIndex) pairs indicating where this variable
     * appears across relations.
     */
    public final ImmutableList<org.apache.calcite.util.Pair<Integer, Integer>> occurrences;

    public JoinVariable(int variableId,
        List<org.apache.calcite.util.Pair<Integer, Integer>> occurrences) {
      this.variableId = variableId;
      this.occurrences = ImmutableList.copyOf(occurrences);
    }

    @Override public String toString() {
      return "Var" + variableId + occurrences;
    }
  }
}
