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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Planner rule that converts a {@link MultiJoin} to an
 * {@link EnumerableWCOJ} (Worst-Case Optimal Join).
 *
 * <p>This rule is applied when:
 * <ul>
 *   <li>WCOJ is enabled via {@link CalciteSystemProperty#ENABLE_WCOJ}</li>
 *   <li>The join graph is cyclic (WCOJ provides benefits for cyclic queries)</li>
 *   <li>Only INNER joins are present</li>
 * </ul>
 */
public class EnumerableWCOJRule extends ConverterRule {

  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(MultiJoin.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableWCOJRule")
      .withRuleFactory(EnumerableWCOJRule::new);

  /** Called from the Config. */
  protected EnumerableWCOJRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final MultiJoin multiJoin = (MultiJoin) rel;

    // Check if WCOJ should be applied
    if (!shouldApplyWCOJ(multiJoin)) {
      return null;
    }

    // Convert all inputs to enumerable convention
    final List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : multiJoin.getInputs()) {
      RelNode newInput = convert(input,
          input.getTraitSet().replace(EnumerableConvention.INSTANCE));
      newInputs.add(newInput);
    }

    // Extract join graph and variables
    final JoinGraph graph = analyzeJoinGraph(multiJoin);
    if (graph == null) {
      return null;  // Could not extract equi-join structure
    }

    // Check for cyclic join graph
    if (!isCyclicJoinGraph(graph, multiJoin.getInputs().size())) {
      return null;  // WCOJ is only beneficial for cyclic queries
    }

    // Build the output row type
    final RelDataType rowType = multiJoin.getRowType();

    return EnumerableWCOJ.create(newInputs, multiJoin.getJoinFilter(),
        graph.variables, rowType);
  }

  /**
   * Determines whether WCOJ should be applied to this MultiJoin.
   */
  private boolean shouldApplyWCOJ(MultiJoin multiJoin) {
    // Check if WCOJ is enabled
    if (!CalciteSystemProperty.ENABLE_WCOJ.value()) {
      return false;
    }

    // Only inner joins are supported
    if (multiJoin.isFullOuterJoin()) {
      return false;
    }

    // Check for outer joins
    for (JoinRelType joinType : multiJoin.getJoinTypes()) {
      if (joinType != JoinRelType.INNER) {
        return false;
      }
    }

    // Need at least 3 inputs for a cyclic query
    if (multiJoin.getInputs().size() < 3) {
      return false;
    }

    return true;
  }

  /**
   * Analyzes the join condition to extract the join graph structure.
   *
   * <p>Extracts equi-join conditions and builds a list of join variables,
   * where each variable represents a column that is equated across relations.
   */
  private @Nullable JoinGraph analyzeJoinGraph(MultiJoin multiJoin) {
    final RexNode joinFilter = multiJoin.getJoinFilter();
    final List<RelNode> inputs = multiJoin.getInputs();

    // Compute field offsets for each input
    final int[] fieldOffsets = new int[inputs.size() + 1];
    fieldOffsets[0] = 0;
    for (int i = 0; i < inputs.size(); i++) {
      fieldOffsets[i + 1] = fieldOffsets[i] +
          inputs.get(i).getRowType().getFieldCount();
    }

    // Extract equi-join conditions
    final List<Pair<Integer, Integer>> equiJoinPairs = new ArrayList<>();
    extractEquiJoinPairs(joinFilter, equiJoinPairs);

    if (equiJoinPairs.isEmpty()) {
      return null;  // No equi-join conditions found
    }

    // Build equivalence classes (union-find)
    final UnionFind uf = new UnionFind(fieldOffsets[inputs.size()]);
    for (Pair<Integer, Integer> pair : equiJoinPairs) {
      uf.union(pair.left, pair.right);
    }

    // Group fields by their equivalence class (variable)
    final Map<Integer, List<Integer>> classToFields = new HashMap<>();
    for (Pair<Integer, Integer> pair : equiJoinPairs) {
      int root = uf.find(pair.left);
      classToFields.computeIfAbsent(root, k -> new ArrayList<>());
      if (!classToFields.get(root).contains(pair.left)) {
        classToFields.get(root).add(pair.left);
      }
      if (!classToFields.get(root).contains(pair.right)) {
        classToFields.get(root).add(pair.right);
      }
    }

    // Convert to JoinVariable objects
    final List<EnumerableWCOJ.JoinVariable> variables = new ArrayList<>();
    int variableId = 0;
    for (List<Integer> fields : classToFields.values()) {
      final List<Pair<Integer, Integer>> occurrences = new ArrayList<>();
      for (Integer field : fields) {
        // Determine which input this field belongs to
        int inputIdx = findInputIndex(field, fieldOffsets);
        int localField = field - fieldOffsets[inputIdx];
        occurrences.add(Pair.of(inputIdx, localField));
      }

      // Only create a variable if it spans multiple inputs
      Set<Integer> inputsInvolved = new HashSet<>();
      for (Pair<Integer, Integer> occ : occurrences) {
        inputsInvolved.add(occ.left);
      }
      if (inputsInvolved.size() >= 2) {
        variables.add(new EnumerableWCOJ.JoinVariable(variableId++, occurrences));
      }
    }

    if (variables.isEmpty()) {
      return null;
    }

    return new JoinGraph(variables);
  }

  /**
   * Finds the input index for a given global field index.
   */
  private int findInputIndex(int globalField, int[] fieldOffsets) {
    for (int i = 0; i < fieldOffsets.length - 1; i++) {
      if (globalField >= fieldOffsets[i] && globalField < fieldOffsets[i + 1]) {
        return i;
      }
    }
    throw new IllegalArgumentException("Field " + globalField + " not found in any input");
  }

  /**
   * Extracts equi-join pairs from a join condition.
   */
  private void extractEquiJoinPairs(RexNode condition,
      List<Pair<Integer, Integer>> pairs) {
    if (condition instanceof RexCall) {
      final RexCall call = (RexCall) condition;
      if (call.getKind() == SqlKind.AND) {
        for (RexNode operand : call.getOperands()) {
          extractEquiJoinPairs(operand, pairs);
        }
      } else if (call.getKind() == SqlKind.EQUALS) {
        final RexNode left = call.getOperands().get(0);
        final RexNode right = call.getOperands().get(1);
        if (left instanceof RexInputRef && right instanceof RexInputRef) {
          final int leftField = ((RexInputRef) left).getIndex();
          final int rightField = ((RexInputRef) right).getIndex();
          pairs.add(Pair.of(leftField, rightField));
        }
      }
    }
  }

  /**
   * Checks if the join graph is cyclic.
   *
   * <p>A join graph is cyclic if the number of edges (equi-join conditions)
   * is greater than or equal to the number of nodes (inputs) for a connected graph.
   * For WCOJ to be beneficial, we specifically look for true cycles.
   */
  private boolean isCyclicJoinGraph(JoinGraph graph, int numInputs) {
    // Build adjacency list
    final Set<Integer>[] adjacency = new Set[numInputs];
    for (int i = 0; i < numInputs; i++) {
      adjacency[i] = new HashSet<>();
    }

    // Add edges from variables
    for (EnumerableWCOJ.JoinVariable var : graph.variables) {
      Set<Integer> inputsInVar = new HashSet<>();
      for (Pair<Integer, Integer> occ : var.occurrences) {
        inputsInVar.add(occ.left);
      }

      // Add edges between all pairs of inputs in this variable
      List<Integer> inputList = new ArrayList<>(inputsInVar);
      for (int i = 0; i < inputList.size(); i++) {
        for (int j = i + 1; j < inputList.size(); j++) {
          adjacency[inputList.get(i)].add(inputList.get(j));
          adjacency[inputList.get(j)].add(inputList.get(i));
        }
      }
    }

    // Count edges (undirected)
    int edgeCount = 0;
    for (int i = 0; i < numInputs; i++) {
      edgeCount += adjacency[i].size();
    }
    edgeCount /= 2;  // Each edge counted twice

    // For a connected graph, it's cyclic if edges >= nodes
    // A tree has exactly (nodes - 1) edges
    return edgeCount >= numInputs;
  }

  /**
   * Represents the join graph structure.
   */
  private static class JoinGraph {
    final List<EnumerableWCOJ.JoinVariable> variables;

    JoinGraph(List<EnumerableWCOJ.JoinVariable> variables) {
      this.variables = variables;
    }
  }

  /**
   * Simple Union-Find data structure for building equivalence classes.
   */
  private static class UnionFind {
    private final int[] parent;
    private final int[] rank;

    UnionFind(int n) {
      parent = new int[n];
      rank = new int[n];
      for (int i = 0; i < n; i++) {
        parent[i] = i;
        rank[i] = 0;
      }
    }

    int find(int x) {
      if (parent[x] != x) {
        parent[x] = find(parent[x]);  // Path compression
      }
      return parent[x];
    }

    void union(int x, int y) {
      int rootX = find(x);
      int rootY = find(y);
      if (rootX != rootY) {
        // Union by rank
        if (rank[rootX] < rank[rootY]) {
          parent[rootX] = rootY;
        } else if (rank[rootX] > rank[rootY]) {
          parent[rootY] = rootX;
        } else {
          parent[rootY] = rootX;
          rank[rootX]++;
        }
      }
    }
  }
}
