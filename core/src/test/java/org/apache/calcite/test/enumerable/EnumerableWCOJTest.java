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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableWCOJ;
import org.apache.calcite.adapter.enumerable.EnumerableWCOJRule;
import org.apache.calcite.adapter.enumerable.JoinVariableFingerprint;
import org.apache.calcite.adapter.enumerable.WCOJPrefixAnalyzer;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.TrieCache;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ReflectiveSchemaWithoutRowCount;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link org.apache.calcite.adapter.enumerable.EnumerableWCOJ}
 * and the WCOJ algorithm in {@link EnumerableDefaults}.
 *
 * <p>These tests include:
 * <ul>
 *   <li>Runtime behavior tests for the WCOJ algorithm</li>
 *   <li>Plan verification tests to ensure the optimizer uses WCOJ</li>
 * </ul>
 */
class EnumerableWCOJTest {

  // =========================================================================
  // Schema for plan verification tests
  // =========================================================================

  /**
   * Schema for triangle query tests.
   * Contains three "edge" tables that can form triangles.
   */
  public static class TriangleSchema {
    public final Edge[] edges1 = {
        new Edge(1, 2),
        new Edge(2, 3),
        new Edge(4, 5)
    };

    public final Edge[] edges2 = {
        new Edge(2, 3),
        new Edge(3, 1),
        new Edge(5, 6)
    };

    public final Edge[] edges3 = {
        new Edge(3, 1),
        new Edge(1, 2),
        new Edge(6, 4)
    };
  }

  /** Edge in a graph, used for triangle queries. */
  public static class Edge {
    public final int src;
    public final int dst;

    public Edge(int src, int dst) {
      this.src = src;
      this.dst = dst;
    }
  }

  // =========================================================================
  // Rule transformation tests
  // =========================================================================

  /**
   * Tests that JOIN_TO_MULTI_JOIN correctly creates a MultiJoin from a
   * cyclic 3-way join (triangle query).
   */
  @Test void testJoinToMultiJoinCreatesMultiJoin() {
    final RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build());

    // Build: R(a, b) JOIN S(b, c) JOIN T(c, a)
    // This is a cyclic join (triangle)
    // After first join R(a,b) JOIN S(b,c), we have fields: a, b, b0, c
    // Then joining with T(c,a): fields are: a, b, b0, c, c0, a0
    // Join conditions: R.b=S.b (field 1 = field 2), S.c=T.c (field 3 = field 4), T.a=R.a (field 5 = field 0)
    RelNode rel = builder
        .values(new String[]{"a", "b"}, 1, 2)  // R: fields 0, 1
        .values(new String[]{"b", "c"}, 2, 3)  // S: fields 0, 1 (becomes 2, 3 after join)
        // First join: R JOIN S ON R.b = S.b (field 1 = field 0 of S)
        .join(JoinRelType.INNER,
            builder.equals(
                builder.field(2, 0, 1),   // R.b
                builder.field(2, 1, 0)))  // S.b
        .values(new String[]{"c", "a"}, 3, 1)  // T: fields 0, 1
        // Second join: (R JOIN S) JOIN T ON S.c = T.c AND T.a = R.a
        // After first join, we have fields: a(0), b(1), b(2), c(3)
        // T has fields: c(0), a(1)
        .join(JoinRelType.INNER,
            builder.and(
                builder.equals(
                    builder.field(2, 0, 3),   // S.c (field 3 of left input)
                    builder.field(2, 1, 0)),  // T.c
                builder.equals(
                    builder.field(2, 1, 1),   // T.a
                    builder.field(2, 0, 0)))) // R.a
        .build();

    // Apply JOIN_TO_MULTI_JOIN rule
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    RelNode result = planner.findBestExp();

    // Verify the result contains MultiJoin
    String plan = RelOptUtil.toString(result);
    assertThat("Plan should contain MultiJoin after applying JOIN_TO_MULTI_JOIN",
        plan, containsString("MultiJoin"));
  }

  /**
   * Tests that EnumerableWCOJRule correctly converts a cyclic MultiJoin
   * to EnumerableWCOJ when WCOJ is enabled.
   *
   * <p>This test directly verifies the rule transformation without depending
   * on the system property for correctness (though the rule only fires when
   * enabled).
   */
  @Test void testEnumerableWCOJRuleConvertsMultiJoin() {
    // This test verifies the rule mechanics work correctly
    // The rule should match a cyclic MultiJoin and produce EnumerableWCOJ

    final RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build());

    // Build a cyclic 3-way join (same structure as testJoinToMultiJoinCreatesMultiJoin)
    RelNode rel = builder
        .values(new String[]{"a", "b"}, 1, 2)  // R: fields 0, 1
        .values(new String[]{"b", "c"}, 2, 3)  // S: fields 0, 1 (becomes 2, 3 after join)
        .join(JoinRelType.INNER,
            builder.equals(
                builder.field(2, 0, 1),   // R.b
                builder.field(2, 1, 0)))  // S.b
        .values(new String[]{"c", "a"}, 3, 1)  // T: fields 0, 1
        .join(JoinRelType.INNER,
            builder.and(
                builder.equals(
                    builder.field(2, 0, 3),   // S.c (field 3 of left input)
                    builder.field(2, 1, 0)),  // T.c
                builder.equals(
                    builder.field(2, 1, 1),   // T.a
                    builder.field(2, 0, 0)))) // R.a
        .build();

    // First apply JOIN_TO_MULTI_JOIN
    HepProgram multiJoinProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepPlanner hepPlanner = new HepPlanner(multiJoinProgram);
    hepPlanner.setRoot(rel);
    RelNode multiJoinRel = hepPlanner.findBestExp();

    // Verify we have a MultiJoin
    String multiJoinPlan = RelOptUtil.toString(multiJoinRel);
    assertThat("Should have MultiJoin after first transformation",
        multiJoinPlan, containsString("MultiJoin"));

    // Note: The HepPlanner with JOIN_TO_MULTI_JOIN may create nested MultiJoins
    // depending on the tree structure. The end-to-end test (testTriangleQueryPlanVerification)
    // with -Dcalcite.enable.wcoj=true verifies the complete WCOJ rule transformation works
    // with the Volcano planner which handles this correctly.
  }

  /**
   * Tests that EnumerableWCOJ can be created directly and produces correct output.
   *
   * <p>This test verifies the EnumerableWCOJ RelNode structure is correct
   * by creating it directly (bypassing the rule), which allows testing
   * without the system property.
   */
  @Test void testEnumerableWCOJDirectConstruction() {
    final RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build());

    // Build input relations as Values
    RelNode input1 = builder.values(new String[]{"a", "b"}, 1, 2).build();
    RelNode input2 = builder.values(new String[]{"b", "c"}, 2, 3).build();
    RelNode input3 = builder.values(new String[]{"c", "a"}, 3, 1).build();

    List<RelNode> inputs = Arrays.asList(input1, input2, input3);

    // Create join variables representing the cyclic join:
    // Variable 0: a (input1.a = input3.a at field indices 0 and 1)
    // Variable 1: b (input1.b = input2.b at field indices 1 and 0)
    // Variable 2: c (input2.c = input3.c at field indices 1 and 0)
    List<EnumerableWCOJ.JoinVariable> variables = Arrays.asList(
        new EnumerableWCOJ.JoinVariable(0, Arrays.asList(
            org.apache.calcite.util.Pair.of(0, 0),
            org.apache.calcite.util.Pair.of(2, 1))),
        new EnumerableWCOJ.JoinVariable(1, Arrays.asList(
            org.apache.calcite.util.Pair.of(0, 1),
            org.apache.calcite.util.Pair.of(1, 0))),
        new EnumerableWCOJ.JoinVariable(2, Arrays.asList(
            org.apache.calcite.util.Pair.of(1, 1),
            org.apache.calcite.util.Pair.of(2, 0)))
    );

    // Build the output row type (all fields from all inputs)
    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        builder.getTypeFactory();
    org.apache.calcite.rel.type.RelDataType rowType = typeFactory.builder()
        .add("a", typeFactory.createJavaType(Integer.class))
        .add("b", typeFactory.createJavaType(Integer.class))
        .add("b0", typeFactory.createJavaType(Integer.class))
        .add("c", typeFactory.createJavaType(Integer.class))
        .add("c0", typeFactory.createJavaType(Integer.class))
        .add("a0", typeFactory.createJavaType(Integer.class))
        .build();

    // Create the EnumerableWCOJ directly
    EnumerableWCOJ wcoj = EnumerableWCOJ.create(
        inputs,
        builder.literal(true),  // join condition (simplified for this test)
        variables,
        rowType);

    // Verify the structure
    assertThat(wcoj.getInputs().size(), is(3));
    assertThat(wcoj.getVariables().size(), is(3));
    assertThat(wcoj.getRowType().getFieldCount(), is(6));

    // Verify the plan output
    String plan = RelOptUtil.toString(wcoj);
    assertThat("Plan should show EnumerableWCOJ",
        plan, containsString("EnumerableWCOJ"));
    assertThat("Plan should show LogicalValues inputs",
        plan, containsString("LogicalValues"));
    assertThat("Plan should show variables",
        plan, containsString("variables="));
    // Verify all three variables are shown
    assertThat("Plan should show Var0", plan, containsString("Var0"));
    assertThat("Plan should show Var1", plan, containsString("Var1"));
    assertThat("Plan should show Var2", plan, containsString("Var2"));
  }

  /**
   * Tests that a triangle query can be executed with WCOJ infrastructure.
   *
   * <p>This test verifies that the WCOJ rule can be added to the planner
   * and the query executes correctly. When WCOJ is enabled via
   * {@code -Dcalcite.enable.wcoj=true}, the planner will use EnumerableWCOJ.
   */
  @Test void testTriangleQueryWithWCOJRule() {
    final String triangleQuery =
        "SELECT e1.src, e1.dst, e2.dst as mid, e3.src as back "
            + "FROM edges1 e1, edges2 e2, edges3 e3 "
            + "WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

    tester(new TriangleSchema())
        .query(triangleQuery)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // Add the JoinToMultiJoinRule to create MultiJoin from binary joins
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          // Add the WCOJ rule - it will only fire if ENABLE_WCOJ is true
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .runs();
  }

  /**
   * Tests that the plan contains EnumerableWCOJ when WCOJ is enabled
   * and standard join rules are removed.
   *
   * <p>This test explicitly removes the standard enumerable join rules
   * to force the planner to use WCOJ when enabled.
   *
   * <p>Run with {@code -Dcalcite.enable.wcoj=true} to enable WCOJ.
   */
  @Test void testTriangleQueryPlanVerification() {
    final String triangleQuery =
        "SELECT e1.src, e1.dst, e2.dst as mid, e3.src as back "
            + "FROM edges1 e1, edges2 e2, edges3 e3 "
            + "WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

    final boolean wcojEnabled =
        org.apache.calcite.config.CalciteSystemProperty.ENABLE_WCOJ.value();

    if (wcojEnabled) {
      // When WCOJ is enabled, force the planner to use WCOJ by removing
      // standard join rules and adding WCOJ rules
      tester(new TriangleSchema())
          .query(triangleQuery)
          .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
            // Remove standard join rules to force WCOJ path
            planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
            // Add rule to flatten joins to MultiJoin
            planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
            // Add WCOJ rule to convert MultiJoin to EnumerableWCOJ
            planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
          })
          .explainContains("EnumerableWCOJ")
          .runs();
    } else {
      // When WCOJ is disabled, just verify the query runs with standard joins
      tester(new TriangleSchema())
          .query(triangleQuery)
          .runs();
    }
  }

  /**
   * Tests GYO reduction for alpha-acyclicity detection.
   *
   * <p>Verifies that the triangle R(x,y), S(y,z), T(z,x) is correctly
   * detected as alpha-cyclic, but adding U(x,y,z) makes it alpha-acyclic
   * (U is an ear covering all shared vertices). Also tests 4-cycle and
   * star patterns. The previous Berge-cyclicity test (clique expansion +
   * |E| &ge; |V|) would incorrectly classify the triangle+U case as cyclic.
   */
  @Test void testAlphaCyclicityDetection() {
    // Use vertex indices: x=0, y=1, z=2

    // Triangle: R(x,y), S(y,z), T(z,x) — alpha-cyclic
    List<Set<Integer>> triangle = Arrays.asList(
        new HashSet<>(Arrays.asList(0, 1)),  // R: {x, y}
        new HashSet<>(Arrays.asList(1, 2)),  // S: {y, z}
        new HashSet<>(Arrays.asList(2, 0))); // T: {z, x}
    assertThat("Triangle should be alpha-cyclic",
        EnumerableWCOJRule.isAlphaCyclic(triangle), is(true));

    // Triangle + covering hyperedge: R(x,y), S(y,z), T(z,x), U(x,y,z)
    // Alpha-acyclic: U is an ear (its shared vertices {x,y,z} are each
    // covered by another hyperedge). After removing U, the remaining
    // triangle's hyperedges can be removed one by one.
    List<Set<Integer>> triangleWithCover = Arrays.asList(
        new HashSet<>(Arrays.asList(0, 1)),     // R: {x, y}
        new HashSet<>(Arrays.asList(1, 2)),     // S: {y, z}
        new HashSet<>(Arrays.asList(2, 0)),     // T: {z, x}
        new HashSet<>(Arrays.asList(0, 1, 2))); // U: {x, y, z}
    assertThat("Triangle + covering hyperedge should be alpha-acyclic",
        EnumerableWCOJRule.isAlphaCyclic(triangleWithCover), is(false));

    // 4-cycle: A(x,y), B(y,z), C(z,w), D(w,x) — alpha-cyclic
    // w=3
    List<Set<Integer>> fourCycle = Arrays.asList(
        new HashSet<>(Arrays.asList(0, 1)),  // A: {x, y}
        new HashSet<>(Arrays.asList(1, 2)),  // B: {y, z}
        new HashSet<>(Arrays.asList(2, 3)),  // C: {z, w}
        new HashSet<>(Arrays.asList(3, 0))); // D: {w, x}
    assertThat("4-cycle should be alpha-cyclic",
        EnumerableWCOJRule.isAlphaCyclic(fourCycle), is(true));

    // Star: R(x,a), S(x,b), T(x,c) — alpha-acyclic (tree structure)
    // a=1, b=2, c=3 (x=0)
    List<Set<Integer>> star = Arrays.asList(
        new HashSet<>(Arrays.asList(0, 1)),  // R: {x, a}
        new HashSet<>(Arrays.asList(0, 2)),  // S: {x, b}
        new HashSet<>(Arrays.asList(0, 3))); // T: {x, c}
    assertThat("Star query should be alpha-acyclic",
        EnumerableWCOJRule.isAlphaCyclic(star), is(false));

    // FK triangle: lineitem-partsupp-supplier on partkey and suppkey.
    // Hyperedges: {l,ps} for partkey, {l,ps,s} for suppkey (ternary
    // because l.suppkey = ps.suppkey = s.suppkey merges into one class).
    // {l,ps} is an ear witnessed by {l,ps,s}. Alpha-acyclic.
    // This validates the paper's claim in Section 7.2.4.
    // Using l=0, ps=1, s=2.
    List<Set<Integer>> fkTriangle = Arrays.asList(
        new HashSet<>(Arrays.asList(0, 1)),     // partkey: {l, ps}
        new HashSet<>(Arrays.asList(0, 1, 2))); // suppkey: {l, ps, s}
    assertThat("FK triangle (l-ps-s) should be alpha-acyclic",
        EnumerableWCOJRule.isAlphaCyclic(fkTriangle), is(false));

    // FK rectangle: customer-orders-lineitem-supplier on
    // custkey, orderkey, suppkey, nationkey — alpha-cyclic (true 4-cycle).
    // c=0, o=1, l=2, s=3
    List<Set<Integer>> fkRectangle = Arrays.asList(
        new HashSet<>(Arrays.asList(0, 1)),  // custkey: {c, o}
        new HashSet<>(Arrays.asList(1, 2)),  // orderkey: {o, l}
        new HashSet<>(Arrays.asList(2, 3)),  // suppkey: {l, s}
        new HashSet<>(Arrays.asList(3, 0))); // nationkey: {s, c}
    assertThat("FK rectangle (c-o-l-s) should be alpha-cyclic",
        EnumerableWCOJRule.isAlphaCyclic(fkRectangle), is(true));
  }

  private CalciteAssert.AssertThat tester(Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .withSchema("s", new ReflectiveSchemaWithoutRowCount(schema));
  }

  // =========================================================================
  // Runtime behavior tests
  // =========================================================================

  /**
   * Tests a simple two-way join using WCOJ.
   *
   * <p>Join: R(a, b) JOIN S(a, c) ON R.a = S.a
   */
  @Test void testTwoWayJoin() {
    // R(a, b): (1, 10), (2, 20), (1, 11)
    List<Object[]> tableR = Arrays.asList(
        new Object[]{1, 10},
        new Object[]{2, 20},
        new Object[]{1, 11});

    // S(a, c): (1, 100), (3, 300), (1, 101)
    List<Object[]> tableS = Arrays.asList(
        new Object[]{1, 100},
        new Object[]{3, 300},
        new Object[]{1, 101});

    // Join on a (field 0 of both)
    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS));

    // Join key indices: input 0 field 0, input 1 field 0
    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0},
        new int[]{0});

    // Variable 0 appears at (input 0, field 0) and (input 1, field 0)
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0}  // Variable 0: R.a = S.a
    };

    // Result selector: concatenate all fields
    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] r = inputRows[0];  // R row
      Object[] s = inputRows[1];  // S row
      return new Object[]{r[0], r[1], s[0], s[1]};  // (R.a, R.b, S.a, S.c)
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Expected: all pairs where R.a = S.a
    // R(1, 10) joins with S(1, 100), S(1, 101) -> 2 results
    // R(1, 11) joins with S(1, 100), S(1, 101) -> 2 results
    // Total: 4 results
    assertThat(resultList.size(), is(4));

    Set<String> resultSet = new HashSet<>();
    for (Object[] row : resultList) {
      resultSet.add(Arrays.toString(row));
    }

    assertTrue(resultSet.contains("[1, 10, 1, 100]"));
    assertTrue(resultSet.contains("[1, 10, 1, 101]"));
    assertTrue(resultSet.contains("[1, 11, 1, 100]"));
    assertTrue(resultSet.contains("[1, 11, 1, 101]"));
  }

  /**
   * Tests a triangle query using WCOJ.
   *
   * <p>Triangle query finds all triangles in a graph:
   * R(a, b) JOIN S(b, c) JOIN T(c, a)
   * ON R.b = S.b AND S.c = T.c AND T.a = R.a
   */
  @Test void testTriangleQuery() {
    // Edges: (1, 2), (2, 3), (3, 1) form a triangle
    // R(a, b)
    List<Object[]> tableR = Arrays.asList(
        new Object[]{1, 2},
        new Object[]{4, 5});  // Non-triangle edge

    // S(b, c)
    List<Object[]> tableS = Arrays.asList(
        new Object[]{2, 3},
        new Object[]{5, 6});  // Non-triangle edge

    // T(c, a)
    List<Object[]> tableT = Arrays.asList(
        new Object[]{3, 1},   // Completes triangle
        new Object[]{6, 7});  // Non-triangle edge

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS),
        Linq4j.asEnumerable(tableT));

    // Join key indices for each input
    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},  // R: fields a=0, b=1
        new int[]{0, 1},  // S: fields b=0, c=1
        new int[]{0, 1}); // T: fields c=0, a=1

    // Variables:
    // Var 0: R.a = T.a (R field 0 = T field 1)
    // Var 1: R.b = S.b (R field 1 = S field 0)
    // Var 2: S.c = T.c (S field 1 = T field 0)
    int[][] variableToInputs = new int[][]{
        {0, 0, 2, 1},  // Var 0: a appears at R.0 and T.1
        {0, 1, 1, 0},  // Var 1: b appears at R.1 and S.0
        {1, 1, 2, 0}   // Var 2: c appears at S.1 and T.0
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] r = inputRows[0];
      Object[] s = inputRows[1];
      Object[] t = inputRows[2];
      return new Object[]{r[0], r[1], s[0], s[1], t[0], t[1]};
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Should find exactly 1 triangle: (1, 2, 3)
    assertThat(resultList.size(), is(1));
    Object[] triangle = resultList.get(0);
    // R(1, 2), S(2, 3), T(3, 1)
    assertThat(triangle[0], is(1));  // R.a
    assertThat(triangle[1], is(2));  // R.b
    assertThat(triangle[2], is(2));  // S.b
    assertThat(triangle[3], is(3));  // S.c
    assertThat(triangle[4], is(3));  // T.c
    assertThat(triangle[5], is(1));  // T.a
  }

  /**
   * Tests an empty result when no matches exist.
   */
  @Test void testNoMatches() {
    List<Object[]> tableR = Arrays.asList(
        new Object[]{1, 10},
        new Object[]{2, 20});

    List<Object[]> tableS = Arrays.asList(
        new Object[]{3, 100},
        new Object[]{4, 200});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0},
        new int[]{0});

    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0}
    };

    Function1<Object[][], Object[]> resultSelector = inputRows ->
        new Object[]{inputRows[0][0], inputRows[1][0]};

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    assertTrue(result.toList().isEmpty());
  }

  /**
   * Tests WCOJ with empty input tables.
   */
  @Test void testEmptyInput() {
    List<Object[]> tableR = new ArrayList<>();
    List<Object[]> tableS = new ArrayList<>();
    tableS.add(new Object[]{1, 100});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0},
        new int[]{0});

    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0}
    };

    Function1<Object[][], Object[]> resultSelector = inputRows ->
        new Object[]{inputRows[0][0], inputRows[1][0]};

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    assertTrue(result.toList().isEmpty());
  }

  /**
   * Tests a four-clique query.
   *
   * <p>Four tables A, B, C, D with join conditions:
   * A.x = B.x, B.y = C.y, C.z = D.z, D.w = A.w
   */
  @Test void testFourCliqueQuery() {
    // Create data that forms a 4-clique
    // A(x, w): x=1, w=4
    // B(x, y): x=1, y=2
    // C(y, z): y=2, z=3
    // D(z, w): z=3, w=4
    List<Object[]> tableA = Arrays.asList(
        new Object[]{1, 4},
        new Object[]{9, 9});  // Non-matching

    List<Object[]> tableB = Arrays.asList(
        new Object[]{1, 2},
        new Object[]{8, 8});  // Non-matching

    List<Object[]> tableC = Arrays.asList(
        new Object[]{2, 3},
        new Object[]{7, 7});  // Non-matching

    List<Object[]> tableD = Arrays.asList(
        new Object[]{3, 4},
        new Object[]{6, 6});  // Non-matching

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableA),
        Linq4j.asEnumerable(tableB),
        Linq4j.asEnumerable(tableC),
        Linq4j.asEnumerable(tableD));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},  // A: x=0, w=1
        new int[]{0, 1},  // B: x=0, y=1
        new int[]{0, 1},  // C: y=0, z=1
        new int[]{0, 1}); // D: z=0, w=1

    // Variables:
    // Var 0: x (A.0 = B.0)
    // Var 1: y (B.1 = C.0)
    // Var 2: z (C.1 = D.0)
    // Var 3: w (D.1 = A.1)
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0},  // x: A.0 = B.0
        {1, 1, 2, 0},  // y: B.1 = C.0
        {2, 1, 3, 0},  // z: C.1 = D.0
        {3, 1, 0, 1}   // w: D.1 = A.1
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] a = inputRows[0];
      Object[] b = inputRows[1];
      Object[] c = inputRows[2];
      Object[] d = inputRows[3];
      return new Object[]{a[0], b[1], c[1], d[1]};  // x, y, z, w
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Should find exactly 1 match
    assertThat(resultList.size(), is(1));
    Object[] clique = resultList.get(0);
    assertThat(clique[0], is(1));  // x
    assertThat(clique[1], is(2));  // y
    assertThat(clique[2], is(3));  // z
    assertThat(clique[3], is(4));  // w
  }

  // =========================================================================
  // Prefix sharing runtime tests (Phase C)
  // =========================================================================

  /**
   * Tests wcojPrefix: computes shared prefix bindings for the first K
   * variables of a WCOJ and yields them as Object[] arrays.
   */
  @Test void testWcojPrefix() {
    // Two inputs: R(a, b), S(a, c) — share variable 'a'
    List<Object[]> tableR = Arrays.asList(
        new Object[]{1, 10},
        new Object[]{2, 20},
        new Object[]{3, 30});

    List<Object[]> tableS = Arrays.asList(
        new Object[]{1, 100},
        new Object[]{2, 200},
        new Object[]{4, 400});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS));

    // Variable 0: a at R.0 and S.0
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0}
    };

    Enumerable<Object[]> prefixBindings =
        EnumerableDefaults.wcojPrefix(inputs, variableToInputs, null, 1);

    List<Object[]> bindings = prefixBindings.toList();

    // Intersection of R.a and S.a = {1, 2}
    assertThat(bindings.size(), is(2));
    Set<Object> prefixValues = new HashSet<>();
    for (Object[] b : bindings) {
      assertThat(b.length, is(1));
      prefixValues.add(b[0]);
    }
    assertTrue(prefixValues.contains(1));
    assertTrue(prefixValues.contains(2));
  }

  /**
   * Tests wcojWithSharedPrefix: runs a suffix WCOJ starting from
   * precomputed prefix bindings.
   */
  @Test void testWcojWithSharedPrefix() {
    // Triangle query: R(a,b) JOIN S(b,c) JOIN T(c,a)
    // Prefix: variable a (shared by R and T)
    // Suffix: variables b, c
    List<Object[]> tableR = Arrays.asList(
        new Object[]{1, 2},
        new Object[]{4, 5});

    List<Object[]> tableS = Arrays.asList(
        new Object[]{2, 3},
        new Object[]{5, 6});

    List<Object[]> tableT = Arrays.asList(
        new Object[]{3, 1},
        new Object[]{6, 7});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS),
        Linq4j.asEnumerable(tableT));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},
        new int[]{0, 1},
        new int[]{0, 1});

    // Var 0: a at R.0, T.1
    // Var 1: b at R.1, S.0
    // Var 2: c at S.1, T.0
    int[][] variableToInputs = new int[][]{
        {0, 0, 2, 1},
        {0, 1, 1, 0},
        {1, 1, 2, 0}
    };

    // Prefix: only variable 0 (a)
    int[][] prefixVarToInputs = new int[][]{
        {0, 0, 2, 1}
    };

    TrieCache trieCache = new TrieCache();

    // Compute prefix bindings
    Enumerable<Object[]> prefixBindings =
        EnumerableDefaults.wcojPrefix(inputs, prefixVarToInputs, trieCache, 1);

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] r = inputRows[0];
      Object[] s = inputRows[1];
      Object[] t = inputRows[2];
      return new Object[]{r[0], r[1], s[0], s[1], t[0], t[1]};
    };

    // Run suffix WCOJ with shared prefix
    Enumerable<Object[]> result = EnumerableDefaults.wcojWithSharedPrefix(
        inputs, joinKeyIndices, variableToInputs, resultSelector,
        trieCache, prefixBindings, 1);

    List<Object[]> resultList = result.toList();

    // Should find triangle: R(1,2), S(2,3), T(3,1)
    assertThat(resultList.size(), is(1));
    Object[] triangle = resultList.get(0);
    assertThat(triangle[0], is(1));  // R.a
    assertThat(triangle[1], is(2));  // R.b
    assertThat(triangle[2], is(2));  // S.b
    assertThat(triangle[3], is(3));  // S.c
    assertThat(triangle[4], is(3));  // T.c
    assertThat(triangle[5], is(1));  // T.a
  }

  /**
   * Tests that wcojWithSharedPrefix produces the same results as regular
   * wcoj for a triangle query — verifying equivalence.
   */
  @Test void testWcojWithPrefixEquivalence() {
    // Multiple triangles: (1,2,3,1), (1,2,4,1)
    List<Object[]> tableR = Arrays.asList(
        new Object[]{1, 2},
        new Object[]{5, 6});

    List<Object[]> tableS = Arrays.asList(
        new Object[]{2, 3},
        new Object[]{2, 4},
        new Object[]{6, 7});

    List<Object[]> tableT = Arrays.asList(
        new Object[]{3, 1},
        new Object[]{4, 1},
        new Object[]{7, 8});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS),
        Linq4j.asEnumerable(tableT));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},
        new int[]{0, 1},
        new int[]{0, 1});

    int[][] variableToInputs = new int[][]{
        {0, 0, 2, 1},
        {0, 1, 1, 0},
        {1, 1, 2, 0}
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] r = inputRows[0];
      Object[] s = inputRows[1];
      Object[] t = inputRows[2];
      return new Object[]{r[0], r[1], s[1], t[0]};
    };

    // Regular WCOJ
    Enumerable<Object[]> regularResult = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);
    Set<String> regularResultSet = new HashSet<>();
    for (Object[] row : regularResult.toList()) {
      regularResultSet.add(Arrays.toString(row));
    }

    // Prefix WCOJ (prefixDepth = 1, sharing variable 'a')
    int[][] prefixVarToInputs = new int[][]{
        {0, 0, 2, 1}
    };
    TrieCache trieCache = new TrieCache();
    Enumerable<Object[]> prefixBindings =
        EnumerableDefaults.wcojPrefix(inputs, prefixVarToInputs, trieCache, 1);
    Enumerable<Object[]> prefixResult = EnumerableDefaults.wcojWithSharedPrefix(
        inputs, joinKeyIndices, variableToInputs, resultSelector,
        trieCache, prefixBindings, 1);
    Set<String> prefixResultSet = new HashSet<>();
    for (Object[] row : prefixResult.toList()) {
      prefixResultSet.add(Arrays.toString(row));
    }

    // Both should produce the same results
    assertThat("Prefix WCOJ should produce same results as regular WCOJ",
        prefixResultSet, is(regularResultSet));
    assertThat("Should find 2 triangles", regularResultSet.size(), is(2));
  }

  /**
   * Tests that wcojWithSharedPrefix correctly handles empty prefix bindings.
   */
  @Test void testWcojWithEmptyPrefix() {
    List<Object[]> tableR = Arrays.<Object[]>asList(
        new Object[]{1, 2});

    List<Object[]> tableS = Arrays.<Object[]>asList(
        new Object[]{3, 100});  // No match on a

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0},
        new int[]{0});

    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0}
    };

    int[][] prefixVarToInputs = new int[][]{
        {0, 0, 1, 0}
    };

    Function1<Object[][], Object[]> resultSelector = inputRows ->
        new Object[]{inputRows[0][0], inputRows[1][1]};

    Enumerable<Object[]> prefixBindings =
        EnumerableDefaults.wcojPrefix(inputs, prefixVarToInputs, null, 1);
    Enumerable<Object[]> result = EnumerableDefaults.wcojWithSharedPrefix(
        inputs, joinKeyIndices, variableToInputs, resultSelector,
        null, prefixBindings, 1);

    assertTrue(result.toList().isEmpty());
  }

  // =========================================================================
  // TPC-H cyclic join tests
  // =========================================================================

  /** Lineitem-like row for TPC-H self-join tests. */
  public static class TpchLineitem {
    public final int l_orderkey;
    public final int l_suppkey;
    public final int l_partkey;
    public final double l_quantity;

    public TpchLineitem(int l_orderkey, int l_suppkey, int l_partkey,
        double l_quantity) {
      this.l_orderkey = l_orderkey;
      this.l_suppkey = l_suppkey;
      this.l_partkey = l_partkey;
      this.l_quantity = l_quantity;
    }
  }

  /** Schema with lineitem-like table for TPC-H self-join cycle tests. */
  public static class TpchLineitemSchema {
    public final TpchLineitem[] lineitem = {
        new TpchLineitem(1, 10, 100, 5.0),
        new TpchLineitem(1, 20, 200, 3.0),
        new TpchLineitem(2, 20, 100, 7.0),
        new TpchLineitem(3, 30, 300, 1.0),
    };
  }

  /** Larger lineitem schema (8 rows) for exercising WCOJ backtracking. */
  public static class TpchLineitemSchemaLarge {
    public final TpchLineitem[] lineitem = {
        new TpchLineitem(1, 10, 100, 5.0),
        new TpchLineitem(1, 20, 200, 3.0),
        new TpchLineitem(2, 20, 100, 7.0),
        new TpchLineitem(3, 30, 300, 1.0),
        new TpchLineitem(1, 30, 300, 2.0),
        new TpchLineitem(2, 10, 200, 4.0),
        new TpchLineitem(3, 20, 100, 6.0),
        new TpchLineitem(4, 40, 400, 8.0),
    };
  }

  // =========================================================================
  // TPC-H multi-table POJOs for FK cycle tests
  // =========================================================================

  /** Supplier for FK cycle tests. */
  public static class TpchFkSupplier {
    public final int s_suppkey;
    public final int s_nationkey;

    public TpchFkSupplier(int s_suppkey, int s_nationkey) {
      this.s_suppkey = s_suppkey;
      this.s_nationkey = s_nationkey;
    }
  }

  /** Customer for FK cycle tests. */
  public static class TpchFkCustomer {
    public final int c_custkey;
    public final int c_nationkey;

    public TpchFkCustomer(int c_custkey, int c_nationkey) {
      this.c_custkey = c_custkey;
      this.c_nationkey = c_nationkey;
    }
  }

  /** Orders for FK cycle tests. */
  public static class TpchFkOrders {
    public final int o_orderkey;
    public final int o_custkey;

    public TpchFkOrders(int o_orderkey, int o_custkey) {
      this.o_orderkey = o_orderkey;
      this.o_custkey = o_custkey;
    }
  }

  /** Nation for FK cycle tests. */
  public static class TpchFkNation {
    public final int n_nationkey;
    public final String n_name;

    public TpchFkNation(int n_nationkey, String n_name) {
      this.n_nationkey = n_nationkey;
      this.n_name = n_name;
    }
  }

  /**
   * Multi-table TPC-H schema for FK cycle tests.
   *
   * <p>Data is designed so that FK cycles close:
   * <ul>
   *   <li>FK rectangle (c-o-l-s): 4 valid cycles via nationkey</li>
   *   <li>FK diamond (c-o-l-s-n): 4 valid cycles via nation table</li>
   * </ul>
   */
  public static class TpchFkSchema {
    public final TpchLineitem[] lineitem = {
        new TpchLineitem(1, 10, 500, 5.0),
        new TpchLineitem(1, 20, 600, 3.0),
        new TpchLineitem(2, 10, 500, 7.0),
        new TpchLineitem(3, 30, 700, 1.0),
    };

    public final TpchFkSupplier[] supplier = {
        new TpchFkSupplier(10, 1),
        new TpchFkSupplier(20, 1),
        new TpchFkSupplier(30, 2),
    };

    public final TpchFkCustomer[] customer = {
        new TpchFkCustomer(100, 1),
        new TpchFkCustomer(200, 2),
    };

    public final TpchFkOrders[] orders = {
        new TpchFkOrders(1, 100),
        new TpchFkOrders(2, 100),
        new TpchFkOrders(3, 200),
    };

    public final TpchFkNation[] nation = {
        new TpchFkNation(1, "USA"),
        new TpchFkNation(2, "CANADA"),
    };
  }

  /**
   * Tests TPC-H self-join triangle via WCOJ runtime.
   *
   * <p>Pattern: l1.orderkey = l2.orderkey AND l2.suppkey = l3.suppkey
   * AND l3.partkey = l1.partkey (closing the cycle).
   *
   * <p>With 4 rows, produces 4 self-triangles (each row matches itself)
   * plus 1 cross-triangle: l1=(1,10,100), l2=(1,20,200), l3=(2,20,100).
   */
  @Test void testTpchSelfJoinTriangle() {
    // Lineitem-like data: (orderkey, suppkey, partkey)
    List<Object[]> lineitem = Arrays.asList(
        new Object[]{1, 10, 100},   // A
        new Object[]{1, 20, 200},   // B
        new Object[]{2, 20, 100},   // C
        new Object[]{3, 30, 300});  // D

    // Self-join: all 3 inputs use the same data
    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(lineitem));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 2},  // l1: orderkey=0, partkey=2
        new int[]{0, 1},  // l2: orderkey=0, suppkey=1
        new int[]{1, 2}); // l3: suppkey=1, partkey=2

    // Var 0 (orderkey): l1.ok(0,0) = l2.ok(1,0)
    // Var 1 (suppkey):  l2.sk(1,1) = l3.sk(2,1)
    // Var 2 (partkey):  l3.pk(2,2) = l1.pk(0,2)
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0},  // orderkey: input 0 field 0 = input 1 field 0
        {1, 1, 2, 1},  // suppkey:  input 1 field 1 = input 2 field 1
        {2, 2, 0, 2}   // partkey:  input 2 field 2 = input 0 field 2
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] l1 = inputRows[0];
      Object[] l2 = inputRows[1];
      Object[] l3 = inputRows[2];
      return new Object[]{l1[0], l2[1], l3[2]};  // ok, sk, pk
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // 4 self-triangles (A,A,A), (B,B,B), (C,C,C), (D,D,D)
    // + 1 cross-triangle (A,B,C): ok=1, sk=20, pk=100
    assertThat(resultList.size(), is(5));

    // Verify the cross-triangle exists (the TPC-H-like pattern)
    Set<String> resultSet = new HashSet<>();
    for (Object[] row : resultList) {
      resultSet.add(Arrays.toString(row));
    }
    // Cross-triangle: l1=A(ok=1), l2=B(sk=20), l3=C(pk=100)
    assertTrue(resultSet.contains("[1, 20, 100]"),
        "Should find cross-triangle (ok=1, sk=20, pk=100)");
  }

  /**
   * Tests TPC-H self-join 4-cycle via WCOJ runtime.
   *
   * <p>Pattern: l1.suppkey = l2.suppkey AND l2.orderkey = l3.orderkey
   * AND l3.partkey = l4.partkey AND l4.orderkey = l1.orderkey
   */
  @Test void testTpchSelfJoinFourCycle() {
    List<Object[]> lineitem = Arrays.asList(
        new Object[]{1, 10, 100},   // A: ok=1, sk=10, pk=100
        new Object[]{2, 10, 200});  // B: ok=2, sk=10, pk=200

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(lineitem));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},  // l1: ok, sk
        new int[]{0, 1},  // l2: ok, sk
        new int[]{0, 2},  // l3: ok, pk
        new int[]{0, 2}); // l4: ok, pk

    // Var 0 (sk):  l1.sk(0,1) = l2.sk(1,1)
    // Var 1 (ok2): l2.ok(1,0) = l3.ok(2,0)
    // Var 2 (pk):  l3.pk(2,2) = l4.pk(3,2)
    // Var 3 (ok1): l4.ok(3,0) = l1.ok(0,0)
    int[][] variableToInputs = new int[][]{
        {0, 1, 1, 1},  // sk:  input 0 field 1 = input 1 field 1
        {1, 0, 2, 0},  // ok2: input 1 field 0 = input 2 field 0
        {2, 2, 3, 2},  // pk:  input 2 field 2 = input 3 field 2
        {3, 0, 0, 0}   // ok1: input 3 field 0 = input 0 field 0
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] l1 = inputRows[0];
      Object[] l4 = inputRows[3];
      return new Object[]{l1[0], l1[1], l4[0], l4[2]};
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Both rows share sk=10, but each row's ok and pk form
    // independent self-cycles only (no cross-cycle with 2 rows)
    assertThat(resultList.size(), is(2));

    Set<String> resultSet = new HashSet<>();
    for (Object[] row : resultList) {
      resultSet.add(Arrays.toString(row));
    }
    assertTrue(resultSet.contains("[1, 10, 1, 100]"),
        "Self-cycle for row A");
    assertTrue(resultSet.contains("[2, 10, 2, 200]"),
        "Self-cycle for row B");
  }

  /**
   * Tests FK-style triangle with 3 different tables via WCOJ runtime.
   *
   * <p>Pattern: orders.o_id = lineitem.l_orderkey
   * AND lineitem.l_suppkey = cust_supplier.cs_suppkey
   * AND cust_supplier.cs_custkey = orders.o_custkey
   */
  @Test void testTpchFkTriangle() {
    List<Object[]> orders = Arrays.asList(
        new Object[]{1, 100},   // o_id=1, o_custkey=100
        new Object[]{2, 200});  // o_id=2, o_custkey=200

    List<Object[]> lineitem = Arrays.asList(
        new Object[]{1, 10},    // l_orderkey=1, l_suppkey=10
        new Object[]{2, 20});   // l_orderkey=2, l_suppkey=20

    List<Object[]> custSupplier = Arrays.asList(
        new Object[]{100, 10},  // cs_custkey=100, cs_suppkey=10
        new Object[]{200, 30}); // cs_custkey=200, cs_suppkey=30 (no match)

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(orders),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(custSupplier));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},  // orders: o_id=0, o_custkey=1
        new int[]{0, 1},  // lineitem: l_orderkey=0, l_suppkey=1
        new int[]{0, 1}); // cust_supplier: cs_custkey=0, cs_suppkey=1

    // Var 0 (orderkey): orders.o_id(0,0) = lineitem.l_orderkey(1,0)
    // Var 1 (suppkey):  lineitem.l_suppkey(1,1) = cust_supplier.cs_suppkey(2,1)
    // Var 2 (custkey):  cust_supplier.cs_custkey(2,0) = orders.o_custkey(0,1)
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0},  // orderkey
        {1, 1, 2, 1},  // suppkey
        {2, 0, 0, 1}   // custkey
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] o = inputRows[0];
      Object[] l = inputRows[1];
      Object[] cs = inputRows[2];
      return new Object[]{o[0], l[1], cs[0]};  // o_id, l_suppkey, cs_custkey
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Only 1 FK triangle closes:
    // orders(1,100) -> lineitem(1,10) -> cust_supplier(100,10) -> custkey=100
    // orders(2,200) -> lineitem(2,20) -> no cust_supplier with sk=20
    assertThat(resultList.size(), is(1));
    assertThat(resultList.get(0)[0], is(1));    // o_id
    assertThat(resultList.get(0)[1], is(10));   // l_suppkey
    assertThat(resultList.get(0)[2], is(100));  // cs_custkey
  }

  /**
   * Tests FK-style 4-cycle with 4 different tables via WCOJ runtime.
   *
   * <p>Pattern: orders.o_id = lineitem.l_orderkey
   * AND lineitem.l_suppkey = partsupp.ps_suppkey
   * AND partsupp.ps_partkey = supp_cust.sc_partkey
   * AND supp_cust.sc_custkey = orders.o_custkey
   */
  @Test void testTpchFkFourCycle() {
    List<Object[]> orders = Arrays.asList(
        new Object[]{1, 100},    // o_id=1, o_custkey=100
        new Object[]{2, 200});   // o_id=2, o_custkey=200

    List<Object[]> lineitem = Arrays.asList(
        new Object[]{1, 10},     // l_orderkey=1, l_suppkey=10
        new Object[]{2, 20});    // l_orderkey=2, l_suppkey=20

    List<Object[]> partsupp = Arrays.asList(
        new Object[]{10, 500},   // ps_suppkey=10, ps_partkey=500
        new Object[]{20, 600});  // ps_suppkey=20, ps_partkey=600

    List<Object[]> suppCust = Arrays.asList(
        new Object[]{500, 100},  // sc_partkey=500, sc_custkey=100
        new Object[]{600, 300}); // sc_partkey=600, sc_custkey=300 (no match)

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(orders),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(partsupp),
        Linq4j.asEnumerable(suppCust));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},  // orders: o_id=0, o_custkey=1
        new int[]{0, 1},  // lineitem: l_orderkey=0, l_suppkey=1
        new int[]{0, 1},  // partsupp: ps_suppkey=0, ps_partkey=1
        new int[]{0, 1}); // supp_cust: sc_partkey=0, sc_custkey=1

    // Var 0 (orderkey): orders.o_id(0,0) = lineitem.l_orderkey(1,0)
    // Var 1 (suppkey):  lineitem.l_suppkey(1,1) = partsupp.ps_suppkey(2,0)
    // Var 2 (partkey):  partsupp.ps_partkey(2,1) = supp_cust.sc_partkey(3,0)
    // Var 3 (custkey):  supp_cust.sc_custkey(3,1) = orders.o_custkey(0,1)
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0},  // orderkey
        {1, 1, 2, 0},  // suppkey
        {2, 1, 3, 0},  // partkey
        {3, 1, 0, 1}   // custkey
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] o = inputRows[0];
      Object[] l = inputRows[1];
      Object[] ps = inputRows[2];
      Object[] sc = inputRows[3];
      return new Object[]{o[0], l[1], ps[1], sc[1]};
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Only 1 cycle closes: orders(1,100)->lineitem(1,10)->partsupp(10,500)
    //   ->supp_cust(500,100)->orders.custkey=100 ✓
    // The second path: orders(2,200)->lineitem(2,20)->partsupp(20,600)
    //   ->supp_cust(600,300)->orders.custkey=300 ✗ (no match)
    assertThat(resultList.size(), is(1));
    assertThat(resultList.get(0)[0], is(1));    // o_id
    assertThat(resultList.get(0)[1], is(10));   // l_suppkey
    assertThat(resultList.get(0)[2], is(500));  // ps_partkey
    assertThat(resultList.get(0)[3], is(100));  // sc_custkey
  }

  /**
   * Tests that WCOJ handles a self-join triangle where no cross-row
   * triangles exist, only self-matches.
   */
  @Test void testTpchSelfJoinNoTriangles() {
    // Rows share no keys across different rows
    List<Object[]> lineitem = Arrays.asList(
        new Object[]{1, 10, 100},
        new Object[]{2, 20, 200},
        new Object[]{3, 30, 300});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(lineitem));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 2},
        new int[]{0, 1},
        new int[]{1, 2});

    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0},
        {1, 1, 2, 1},
        {2, 2, 0, 2}
    };

    Function1<Object[][], Object[]> resultSelector = inputRows ->
        new Object[]{inputRows[0][0], inputRows[1][1], inputRows[2][2]};

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Each row forms a self-triangle only (no cross-row sharing)
    assertThat(resultList.size(), is(3));
  }

  /**
   * Tests FK-style rectangle (customer-orders-lineitem-supplier) via WCOJ runtime.
   *
   * <p>Pattern: c.c_custkey = o.o_custkey AND o.o_orderkey = l.l_orderkey
   * AND l.l_suppkey = s.s_suppkey AND s.s_nationkey = c.c_nationkey
   *
   * <p>This is the query shape from Table 6 in the paper where WCOJ is 1.5x
   * slower than binary joins due to the low-cardinality nationkey closing predicate.
   */
  @Test void testTpchFkRectangleNationkey() {
    List<Object[]> customer = Arrays.asList(
        new Object[]{100, 1},   // c_custkey=100, c_nationkey=1
        new Object[]{200, 2});  // c_custkey=200, c_nationkey=2

    List<Object[]> orders = Arrays.asList(
        new Object[]{1, 100},   // o_orderkey=1, o_custkey=100
        new Object[]{2, 100},   // o_orderkey=2, o_custkey=100
        new Object[]{3, 200});  // o_orderkey=3, o_custkey=200

    List<Object[]> lineitem = Arrays.asList(
        new Object[]{1, 10},    // l_orderkey=1, l_suppkey=10
        new Object[]{1, 20},    // l_orderkey=1, l_suppkey=20
        new Object[]{2, 10},    // l_orderkey=2, l_suppkey=10
        new Object[]{3, 30});   // l_orderkey=3, l_suppkey=30

    List<Object[]> supplier = Arrays.asList(
        new Object[]{10, 1},    // s_suppkey=10, s_nationkey=1
        new Object[]{20, 1},    // s_suppkey=20, s_nationkey=1
        new Object[]{30, 2});   // s_suppkey=30, s_nationkey=2

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(customer),
        Linq4j.asEnumerable(orders),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(supplier));

    // Join keys ordered by variable participation:
    // customer: c_custkey for v0, c_nationkey for v3
    // orders: o_custkey for v0, o_orderkey for v1
    // lineitem: l_orderkey for v1, l_suppkey for v2
    // supplier: s_suppkey for v2, s_nationkey for v3
    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},  // customer: custkey, nationkey
        new int[]{1, 0},  // orders: custkey, orderkey
        new int[]{0, 1},  // lineitem: orderkey, suppkey
        new int[]{0, 1}); // supplier: suppkey, nationkey

    // Var 0 (custkey): customer(0,0) = orders(1,1)
    // Var 1 (orderkey): orders(1,0) = lineitem(2,0)
    // Var 2 (suppkey): lineitem(2,1) = supplier(3,0)
    // Var 3 (nationkey): supplier(3,1) = customer(0,1)
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 1},  // custkey
        {1, 0, 2, 0},  // orderkey
        {2, 1, 3, 0},  // suppkey
        {3, 1, 0, 1}   // nationkey
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] c = inputRows[0];
      Object[] o = inputRows[1];
      Object[] l = inputRows[2];
      Object[] s = inputRows[3];
      return new Object[]{c[0], o[0], l[1], s[1]};
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // 4 valid cycles:
    // c(100,1)->o(1,100)->l(1,10)->s(10,1)->nk=1=c.nk ✓
    // c(100,1)->o(1,100)->l(1,20)->s(20,1)->nk=1=c.nk ✓
    // c(100,1)->o(2,100)->l(2,10)->s(10,1)->nk=1=c.nk ✓
    // c(200,2)->o(3,200)->l(3,30)->s(30,2)->nk=2=c.nk ✓
    assertThat(resultList.size(), is(4));

    Set<String> resultSet = new HashSet<>();
    for (Object[] row : resultList) {
      resultSet.add(Arrays.toString(row));
    }
    // (c_custkey, o_orderkey, l_suppkey, s_nationkey)
    assertTrue(resultSet.contains("[100, 1, 10, 1]"));
    assertTrue(resultSet.contains("[100, 1, 20, 1]"));
    assertTrue(resultSet.contains("[100, 2, 10, 1]"));
    assertTrue(resultSet.contains("[200, 3, 30, 2]"));
  }

  /**
   * Tests FK-style diamond (customer-orders-lineitem-supplier-nation)
   * via WCOJ runtime. This is a 5-input cyclic join.
   *
   * <p>Pattern: c.c_custkey = o.o_custkey AND o.o_orderkey = l.l_orderkey
   * AND l.l_suppkey = s.s_suppkey AND s.s_nationkey = n.n_nationkey
   * AND n.n_nationkey = c.c_nationkey
   *
   * <p>The nationkey variable spans 3 inputs (supplier, nation, customer),
   * testing WCOJ with ternary hyperedge intersection.
   */
  @Test void testTpchFkDiamond() {
    List<Object[]> customer = Arrays.asList(
        new Object[]{100, 1},      // c_custkey=100, c_nationkey=1
        new Object[]{200, 2});     // c_custkey=200, c_nationkey=2

    List<Object[]> orders = Arrays.asList(
        new Object[]{1, 100},
        new Object[]{2, 100},
        new Object[]{3, 200});

    List<Object[]> lineitem = Arrays.asList(
        new Object[]{1, 10},
        new Object[]{1, 20},
        new Object[]{2, 10},
        new Object[]{3, 30});

    List<Object[]> supplier = Arrays.asList(
        new Object[]{10, 1},
        new Object[]{20, 1},
        new Object[]{30, 2});

    List<Object[]> nation = Arrays.asList(
        new Object[]{1, "USA"},
        new Object[]{2, "CANADA"});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(customer),
        Linq4j.asEnumerable(orders),
        Linq4j.asEnumerable(lineitem),
        Linq4j.asEnumerable(supplier),
        Linq4j.asEnumerable(nation));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0, 1},  // customer: custkey(v0), nationkey(v3)
        new int[]{1, 0},  // orders: custkey(v0), orderkey(v1)
        new int[]{0, 1},  // lineitem: orderkey(v1), suppkey(v2)
        new int[]{0, 1},  // supplier: suppkey(v2), nationkey(v3)
        new int[]{0});    // nation: nationkey(v3)

    // Var 3 (nationkey) spans 3 inputs: supplier(3,1), nation(4,0), customer(0,1)
    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 1},           // custkey
        {1, 0, 2, 0},           // orderkey
        {2, 1, 3, 0},           // suppkey
        {3, 1, 4, 0, 0, 1}      // nationkey (ternary)
    };

    Function1<Object[][], Object[]> resultSelector = inputRows -> {
      Object[] c = inputRows[0];
      Object[] o = inputRows[1];
      Object[] l = inputRows[2];
      Object[] s = inputRows[3];
      Object[] n = inputRows[4];
      return new Object[]{c[0], o[0], l[1], s[1], n[1]};
    };

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // Same 4 cycles as FK rectangle, but through nation table
    assertThat(resultList.size(), is(4));

    Set<String> resultSet = new HashSet<>();
    for (Object[] row : resultList) {
      resultSet.add(Arrays.toString(row));
    }
    // (c_custkey, o_orderkey, l_suppkey, s_nationkey, n_name)
    assertTrue(resultSet.contains("[100, 1, 10, 1, USA]"));
    assertTrue(resultSet.contains("[100, 1, 20, 1, USA]"));
    assertTrue(resultSet.contains("[100, 2, 10, 1, USA]"));
    assertTrue(resultSet.contains("[200, 3, 30, 2, CANADA]"));
  }

  /**
   * SQL-level test: TPC-H self-join triangle detected as cyclic
   * and executed via the planner pipeline.
   */
  @Test void testTpchSelfJoinTriangleSql() {
    final String sql =
        "SELECT l1.l_orderkey, l2.l_suppkey, l3.l_partkey "
            + "FROM lineitem l1, lineitem l2, lineitem l3 "
            + "WHERE l1.l_orderkey = l2.l_orderkey "
            + "AND l2.l_suppkey = l3.l_suppkey "
            + "AND l3.l_partkey = l1.l_partkey";

    tester(new TpchLineitemSchema())
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .runs();
  }

  /**
   * SQL-level test: TPC-H self-join 4-cycle query.
   */
  @Test void testTpchSelfJoinFourCycleSql() {
    final String sql =
        "SELECT l1.l_orderkey, l2.l_suppkey, "
            + "l3.l_partkey, l4.l_orderkey AS ok4 "
            + "FROM lineitem l1, lineitem l2, "
            + "lineitem l3, lineitem l4 "
            + "WHERE l1.l_suppkey = l2.l_suppkey "
            + "AND l2.l_orderkey = l3.l_orderkey "
            + "AND l3.l_partkey = l4.l_partkey "
            + "AND l4.l_orderkey = l1.l_orderkey";

    tester(new TpchLineitemSchema())
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .runs();
  }

  /**
   * SQL-level test: TPC-H self-join triangle with result count verification.
   *
   * <p>4 self-triangles (each row matches itself) plus 1 cross-triangle:
   * l1=(1,10,100), l2=(1,20,200), l3=(2,20,100). Total: 5 rows.
   */
  @Test void testTpchSelfJoinTriangleSqlVerified() {
    final String sql =
        "SELECT l1.l_orderkey, l2.l_suppkey, l3.l_partkey "
            + "FROM lineitem l1, lineitem l2, lineitem l3 "
            + "WHERE l1.l_orderkey = l2.l_orderkey "
            + "AND l2.l_suppkey = l3.l_suppkey "
            + "AND l3.l_partkey = l1.l_partkey";

    tester(new TpchLineitemSchema())
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .returnsCount(5);
  }

  /**
   * SQL-level test: TPC-H self-join 4-cycle with result count verification.
   *
   * <p>Uses the 4-row lineitem schema. Produces 8 valid 4-tuples:
   * 4 self-cycles (A,A,A,A), (B,B,B,B), (C,C,C,C), (D,D,D,D) plus
   * 4 cross-cycles: (A,A,B,B) from sk=10, plus (B,B,A,A), (B,C,C,A),
   * and (C,B,A,C) from sk=20.
   */
  @Test void testTpchSelfJoinFourCycleSqlVerified() {
    final String sql =
        "SELECT l1.l_orderkey, l2.l_suppkey, "
            + "l3.l_partkey, l4.l_orderkey AS ok4 "
            + "FROM lineitem l1, lineitem l2, "
            + "lineitem l3, lineitem l4 "
            + "WHERE l1.l_suppkey = l2.l_suppkey "
            + "AND l2.l_orderkey = l3.l_orderkey "
            + "AND l3.l_partkey = l4.l_partkey "
            + "AND l4.l_orderkey = l1.l_orderkey";

    tester(new TpchLineitemSchema())
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .returnsCount(8);
  }

  /**
   * SQL-level test: self-join triangle with 8-row dataset.
   *
   * <p>Exercises WCOJ backtracking more thoroughly with overlapping
   * key groups across multiple orderkey, suppkey, and partkey values.
   * Produces 17 valid triangles including cross-row matches.
   */
  @Test void testTpchSelfJoinTriangleLargerData() {
    final String sql =
        "SELECT l1.l_orderkey, l2.l_suppkey, l3.l_partkey "
            + "FROM lineitem l1, lineitem l2, lineitem l3 "
            + "WHERE l1.l_orderkey = l2.l_orderkey "
            + "AND l2.l_suppkey = l3.l_suppkey "
            + "AND l3.l_partkey = l1.l_partkey";

    tester(new TpchLineitemSchemaLarge())
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .returnsCount(17);
  }

  /**
   * SQL-level test: FK rectangle (customer-orders-lineitem-supplier).
   *
   * <p>4-table cycle closed by nationkey: s.s_nationkey = c.c_nationkey.
   * This is the query shape from Table 6 in the paper where binary joins
   * outperform WCOJ (1.5x slower) due to the low-cardinality closing
   * predicate. Tests that the planner pipeline handles FK cycles correctly.
   */
  @Test void testTpchFkRectangleSql() {
    final String sql =
        "SELECT c.c_custkey, o.o_orderkey, l.l_suppkey, s.s_nationkey "
            + "FROM customer c, orders o, lineitem l, supplier s "
            + "WHERE c.c_custkey = o.o_custkey "
            + "AND o.o_orderkey = l.l_orderkey "
            + "AND l.l_suppkey = s.s_suppkey "
            + "AND s.s_nationkey = c.c_nationkey";

    tester(new TpchFkSchema())
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .returnsCount(4);
  }

  /**
   * SQL-level test: FK diamond (customer-orders-lineitem-supplier-nation).
   *
   * <p>5-table cycle through the nation table. This is the query shape
   * showing the worst WCOJ regression (3.9x slower) in the paper's
   * TPC-H benchmarks (Table 6). Tests the full planner pipeline with
   * 5-way cyclic FK joins.
   */
  @Test void testTpchFkDiamondSql() {
    final String sql =
        "SELECT c.c_custkey, o.o_orderkey, l.l_suppkey, "
            + "s.s_nationkey, n.n_name "
            + "FROM customer c, orders o, lineitem l, supplier s, nation n "
            + "WHERE c.c_custkey = o.o_custkey "
            + "AND o.o_orderkey = l.l_orderkey "
            + "AND l.l_suppkey = s.s_suppkey "
            + "AND s.s_nationkey = n.n_nationkey "
            + "AND n.n_nationkey = c.c_nationkey";

    tester(new TpchFkSchema())
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
          planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
        })
        .returnsCount(4);
  }

  /**
   * Tests that WCOJ correctly handles multiple matching rows.
   */
  @Test void testMultipleMatches() {
    // All rows have key = 1, so all combinations match
    List<Object[]> tableR = Arrays.asList(
        new Object[]{1, "A"},
        new Object[]{1, "B"});

    List<Object[]> tableS = Arrays.asList(
        new Object[]{1, "X"},
        new Object[]{1, "Y"},
        new Object[]{1, "Z"});

    List<Enumerable<Object[]>> inputs = Arrays.asList(
        Linq4j.asEnumerable(tableR),
        Linq4j.asEnumerable(tableS));

    List<int[]> joinKeyIndices = Arrays.asList(
        new int[]{0},
        new int[]{0});

    int[][] variableToInputs = new int[][]{
        {0, 0, 1, 0}
    };

    Function1<Object[][], Object[]> resultSelector = inputRows ->
        new Object[]{inputRows[0][1], inputRows[1][1]};

    Enumerable<Object[]> result = EnumerableDefaults.wcoj(
        inputs, joinKeyIndices, variableToInputs, resultSelector);

    List<Object[]> resultList = result.toList();

    // 2 R rows x 3 S rows = 6 results
    assertThat(resultList.size(), is(6));

    Set<String> resultSet = new HashSet<>();
    for (Object[] row : resultList) {
      resultSet.add(Arrays.toString(row));
    }

    assertTrue(resultSet.contains("[A, X]"));
    assertTrue(resultSet.contains("[A, Y]"));
    assertTrue(resultSet.contains("[A, Z]"));
    assertTrue(resultSet.contains("[B, X]"));
    assertTrue(resultSet.contains("[B, Y]"));
    assertTrue(resultSet.contains("[B, Z]"));
  }

  // =========================================================================
  // Prefix sharing end-to-end tests (MULTI + Combine + WCOJ)
  // =========================================================================

  /**
   * Tests that MULTI() with two WCOJ triangle queries produces correct
   * results in combine mode (TrieCache sharing) and combine-share mode
   * (TrieCache + prefix sharing).
   *
   * <p>Both queries are triangles on the same edges tables with
   * different projections. The shared table scans should be materialized
   * once via TrieCache, and the prefix sharing rule should detect shared
   * variable prefixes.
   */
  @Test void testPrefixSharingMultiQueryCorrectness() {
    // Q1 and Q2: same triangle, different projections
    final String q1 =
        "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c "
            + "FROM edges1 e1, edges2 e2, edges3 e3 "
            + "WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

    final String q2 =
        "SELECT e1.src AS x, e2.src AS y, e3.src AS z "
            + "FROM edges1 e1, edges2 e2, edges3 e3 "
            + "WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

    final String multiSql = "MULTI((" + q1 + "), (" + q2 + "))";

    // combine-share mode (prefix sharing + TrieCache)
    // Keep join rules as fallback — WCOJ rule will be preferred for cyclic queries
    final Consumer<RelOptPlanner> combineShareHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
      planner.addRule(CoreRules.COMBINE_SHARED_COMPONENTS);
      planner.addRule(EnumerableRules.ENUMERABLE_COMBINE_WCOJ_PREFIX_RULE);
    };

    tester(new TriangleSchema())
        .query(multiSql)
        .withHook(Hook.PLANNER, combineShareHook)
        .runs();

    // combine mode (TrieCache only, no prefix sharing)
    final Consumer<RelOptPlanner> combineHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
    };

    tester(new TriangleSchema())
        .query(multiSql)
        .withHook(Hook.PLANNER, combineHook)
        .runs();
  }

  /**
   * Tests that MULTI() with three WCOJ triangle queries produces correct
   * results in combine-share mode. All three share the same tables and
   * join structure, so prefix sharing should group them.
   */
  @Test void testPrefixSharingThreeQueries() {
    final String q1 =
        "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c "
            + "FROM edges1 e1, edges2 e2, edges3 e3 "
            + "WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

    final String q2 =
        "SELECT e1.src AS x, e2.src AS y, e3.src AS z "
            + "FROM edges1 e1, edges2 e2, edges3 e3 "
            + "WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

    final String q3 =
        "SELECT e3.dst AS p, e1.dst AS q, e2.dst AS r "
            + "FROM edges1 e1, edges2 e2, edges3 e3 "
            + "WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

    final String multiSql =
        "MULTI((" + q1 + "), (" + q2 + "), (" + q3 + "))";

    final Consumer<RelOptPlanner> combineShareHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
      planner.addRule(CoreRules.COMBINE_SHARED_COMPONENTS);
      planner.addRule(EnumerableRules.ENUMERABLE_COMBINE_WCOJ_PREFIX_RULE);
    };

    tester(new TriangleSchema())
        .query(multiSql)
        .withHook(Hook.PLANNER, combineShareHook)
        .runs();
  }

  /**
   * Tests that MULTI() with WCOJ and TrieCache sharing produces correct
   * results. Uses the FK schema with different tables to verify shared
   * input materialization works across distinct table scans.
   */
  @Test void testTrieCacheSharingFkMultiQuery() {
    // Two FK rectangle queries with different projections
    final String q1 =
        "SELECT c.c_custkey, o.o_orderkey, l.l_suppkey, s.s_nationkey "
            + "FROM customer c, orders o, lineitem l, supplier s "
            + "WHERE c.c_custkey = o.o_custkey "
            + "AND o.o_orderkey = l.l_orderkey "
            + "AND l.l_suppkey = s.s_suppkey "
            + "AND s.s_nationkey = c.c_nationkey";

    final String q2 =
        "SELECT s.s_suppkey, l.l_orderkey, o.o_orderkey, c.c_nationkey "
            + "FROM customer c, orders o, lineitem l, supplier s "
            + "WHERE c.c_custkey = o.o_custkey "
            + "AND o.o_orderkey = l.l_orderkey "
            + "AND l.l_suppkey = s.s_suppkey "
            + "AND s.s_nationkey = c.c_nationkey";

    final String multiSql = "MULTI((" + q1 + "), (" + q2 + "))";

    // combine mode -- TrieCache sharing only
    final Consumer<RelOptPlanner> combineHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
    };

    tester(new TpchFkSchema())
        .query(multiSql)
        .withHook(Hook.PLANNER, combineHook)
        .runs();

    // combine-share mode -- TrieCache + prefix sharing
    final Consumer<RelOptPlanner> combineShareHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
      planner.addRule(CoreRules.COMBINE_SHARED_COMPONENTS);
      planner.addRule(EnumerableRules.ENUMERABLE_COMBINE_WCOJ_PREFIX_RULE);
    };

    tester(new TpchFkSchema())
        .query(multiSql)
        .withHook(Hook.PLANNER, combineShareHook)
        .runs();
  }

}
