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
package org.apache.calcite.test;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Command-line benchmark comparing baseline binary joins vs WCOJ
 * on cyclic queries over a dense graph dataset.
 *
 * <p>Supports three query shapes:
 * <ul>
 *   <li><b>triangle</b>: 3-cycle a→b→c→a (3 tables, 3 variables)</li>
 *   <li><b>rectangle</b>: 4-cycle a→b→c→d→a (4 tables, 4 variables)</li>
 *   <li><b>diamond</b>: two triangles sharing edge a→b (5 tables, 4 variables)</li>
 * </ul>
 *
 * <p>Requires {@code -Dcalcite.enable.wcoj=true} JVM argument.
 *
 * <p>Usage:
 * <pre>
 *   ./run-wcoj-benchmark.sh [options]
 *
 * Options:
 *   --nodes=N        Number of graph vertices (default: 200)
 *   --edges=N        Number of directed edges (default: 1000)
 *   --queries=N      Query variations for multi modes (default: 5)
 *   --warmup=N       Warmup iterations (default: 3)
 *   --iterations=N   Measurement iterations (default: 10)
 *   --mode=MODE      Only run: baseline|wcoj|combine|combine-share
 *   --shape=SHAPE    Query shape: triangle|rectangle|diamond|all (default: all)
 *   --seed=N         Random seed for graph generation (default: 42)
 *   --csv            Output in CSV format
 *   --verbose        Show per-iteration details
 * </pre>
 */
public class WCOJBenchmarkCli {

  // Configuration
  private int numNodes = 200;
  private int numEdges = 1000;
  private int queryCount = 5;
  private int warmupIterations = 3;
  private int measureIterations = 10;
  private String modeFilter = null; // null = run all
  private String shapeFilter = "all";
  private long seed = 42;
  private boolean csvOutput = false;
  private boolean verbose = true;

  private static final String[] ALL_SHAPES = {"triangle", "rectangle", "diamond"};

  public static void main(String[] args) throws Exception {
    // Verify WCOJ is enabled (set via -Dcalcite.enable.wcoj=true JVM arg)
    boolean wcojEnabled =
        org.apache.calcite.config.CalciteSystemProperty.ENABLE_WCOJ.value();
    if (!wcojEnabled) {
      System.err.println("WARNING: calcite.enable.wcoj is not set. "
          + "WCOJ rules will not fire. Pass -Dcalcite.enable.wcoj=true");
    }

    DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());

    WCOJBenchmarkCli cli = new WCOJBenchmarkCli();
    cli.parseArgs(args);
    cli.run();
  }

  private void parseArgs(String[] args) {
    for (String arg : args) {
      if (arg.startsWith("--nodes=")) {
        numNodes = Integer.parseInt(arg.substring("--nodes=".length()));
      } else if (arg.startsWith("--edges=")) {
        numEdges = Integer.parseInt(arg.substring("--edges=".length()));
      } else if (arg.startsWith("--queries=")) {
        queryCount = Integer.parseInt(arg.substring("--queries=".length()));
      } else if (arg.startsWith("--warmup=")) {
        warmupIterations = Integer.parseInt(arg.substring("--warmup=".length()));
      } else if (arg.startsWith("--iterations=")) {
        measureIterations = Integer.parseInt(arg.substring("--iterations=".length()));
      } else if (arg.startsWith("--mode=")) {
        modeFilter = arg.substring("--mode=".length());
      } else if (arg.startsWith("--shape=")) {
        shapeFilter = arg.substring("--shape=".length());
      } else if (arg.startsWith("--seed=")) {
        seed = Long.parseLong(arg.substring("--seed=".length()));
      } else if (arg.equals("--csv")) {
        csvOutput = true;
        verbose = false;
      } else if (arg.equals("--verbose")) {
        verbose = true;
      } else if (arg.equals("--help") || arg.equals("-h")) {
        printUsage();
        System.exit(0);
      }
    }
  }

  private void printUsage() {
    System.out.println("WCOJ Benchmark — Baseline vs WCOJ vs Multi-Query WCOJ");
    System.out.println();
    System.out.println("Usage: ./run-wcoj-benchmark.sh [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --nodes=N        Number of graph vertices (default: 200)");
    System.out.println("  --edges=N        Number of directed edges (default: 1000)");
    System.out.println("  --queries=N      Query variations for multi modes (default: 5)");
    System.out.println("  --warmup=N       Warmup iterations (default: 3)");
    System.out.println("  --iterations=N   Measurement iterations (default: 10)");
    System.out.println("  --mode=MODE      Only run: baseline|wcoj|combine|combine-share");
    System.out.println("  --shape=SHAPE    Query shape: triangle|rectangle|diamond|all (default: all)");
    System.out.println("  --seed=N         Random seed for graph generation (default: 42)");
    System.out.println("  --csv            Output results in CSV format");
    System.out.println("  --verbose        Show per-iteration timing details");
    System.out.println("  --help, -h       Show this help message");
  }

  // ---------------------------------------------------------------
  // Graph schema — directed edges with weight
  // ---------------------------------------------------------------

  /** A directed edge with src, dst, and weight. */
  public static class GraphEdge {
    public final int src;
    public final int dst;
    public final double weight;

    public GraphEdge(int src, int dst, double weight) {
      this.src = src;
      this.dst = dst;
      this.weight = weight;
    }
  }

  /** Schema with six edge tables for cyclic query shapes.
   *  All point to the same edge data (self-join over one graph).
   *  Triangle uses edges1-3, rectangle uses edges1-4, diamond uses edges1-5. */
  public static class GraphSchema {
    public final GraphEdge[] edges1;
    public final GraphEdge[] edges2;
    public final GraphEdge[] edges3;
    public final GraphEdge[] edges4;
    public final GraphEdge[] edges5;
    public final GraphEdge[] edges6;

    GraphSchema(GraphEdge[] edges) {
      this.edges1 = edges;
      this.edges2 = edges;
      this.edges3 = edges;
      this.edges4 = edges;
      this.edges5 = edges;
      this.edges6 = edges;
    }
  }

  /**
   * Generate a "dense hub" graph designed to stress binary joins.
   *
   * <p>Creates hub nodes that connect to many spokes. Binary join
   * R(a,b) JOIN S(b,c) on b=hub produces O(fanIn * fanOut) intermediate
   * rows per hub, but only a fraction close the cycle.
   */
  private GraphEdge[] generateDenseHubGraph(int nodes, int targetEdges, long graphSeed) {
    Random rng = new Random(graphSeed);
    Set<Long> seen = new HashSet<>();
    List<GraphEdge> edgeList = new ArrayList<>();

    // Pick ~5% of nodes as hubs
    int numHubs = Math.max(2, nodes / 20);
    int[] hubs = new int[numHubs];
    for (int i = 0; i < numHubs; i++) {
      hubs[i] = i; // First few nodes are hubs
    }

    // Each hub gets many incoming and outgoing edges
    int edgesPerHub = targetEdges / (numHubs * 2);
    for (int hub : hubs) {
      for (int j = 0; j < edgesPerHub; j++) {
        int spoke = numHubs + rng.nextInt(nodes - numHubs);
        // hub → spoke
        long key1 = (long) hub * nodes + spoke;
        if (seen.add(key1)) {
          edgeList.add(new GraphEdge(hub, spoke, rng.nextDouble() * 100.0));
        }
        // spoke → hub
        long key2 = (long) spoke * nodes + hub;
        if (seen.add(key2)) {
          edgeList.add(new GraphEdge(spoke, hub, rng.nextDouble() * 100.0));
        }
      }
    }

    // Add some spoke-to-spoke edges to create cycles
    int spokeEdges = targetEdges - edgeList.size();
    for (int j = 0; j < spokeEdges && j < nodes * 2; j++) {
      int a = numHubs + rng.nextInt(nodes - numHubs);
      int b = numHubs + rng.nextInt(nodes - numHubs);
      if (a != b) {
        long key = (long) a * nodes + b;
        if (seen.add(key)) {
          edgeList.add(new GraphEdge(a, b, rng.nextDouble() * 100.0));
        }
      }
    }

    return edgeList.toArray(new GraphEdge[0]);
  }

  private void printGraphStats(List<GraphEdge> edgeList, String shape) {
    if (csvOutput) {
      return;
    }
    int numHubs = Math.max(2, numNodes / 20);
    System.out.printf("  Graph:        %d edges, %d hubs%n", edgeList.size(), numHubs);
    if (numNodes <= 500) {
      switch (shape) {
      case "triangle":
        System.out.printf("  Triangles:    ~%d%n", countTriangles(edgeList));
        break;
      case "rectangle":
        System.out.printf("  Rectangles:   ~%d%n", countRectangles(edgeList));
        break;
      case "diamond":
        System.out.printf("  Diamonds:     ~%d%n", countDiamonds(edgeList));
        break;
      default:
        break;
      }
    }
    long intermediateEst = estimateIntermediateSize(edgeList, numNodes);
    System.out.printf("  Binary join intermediate est: ~%,d rows%n", intermediateEst);
  }

  /** Count actual triangles in the edge list: a→b→c→a. */
  private int countTriangles(List<GraphEdge> edges) {
    Set<Long> edgeSet = new HashSet<>();
    int maxNode = 0;
    for (GraphEdge e : edges) {
      edgeSet.add((long) e.src * 100000 + e.dst);
      maxNode = Math.max(maxNode, Math.max(e.src, e.dst));
    }
    @SuppressWarnings("unchecked")
    List<Integer>[] adj = new List[maxNode + 1];
    for (int i = 0; i <= maxNode; i++) {
      adj[i] = new ArrayList<>();
    }
    for (GraphEdge e : edges) {
      adj[e.src].add(e.dst);
    }
    int count = 0;
    for (GraphEdge e : edges) {
      int a = e.src;
      int b = e.dst;
      for (int c : adj[b]) {
        if (edgeSet.contains((long) c * 100000 + a)) {
          count++;
        }
      }
    }
    return count;
  }

  /** Count 4-cycles (rectangles): a→b→c→d→a. */
  private int countRectangles(List<GraphEdge> edges) {
    Set<Long> edgeSet = new HashSet<>();
    int maxNode = 0;
    for (GraphEdge e : edges) {
      edgeSet.add((long) e.src * 100000 + e.dst);
      maxNode = Math.max(maxNode, Math.max(e.src, e.dst));
    }
    @SuppressWarnings("unchecked")
    List<Integer>[] adj = new List[maxNode + 1];
    for (int i = 0; i <= maxNode; i++) {
      adj[i] = new ArrayList<>();
    }
    for (GraphEdge e : edges) {
      adj[e.src].add(e.dst);
    }
    long count = 0;
    for (GraphEdge e : edges) {
      int a = e.src;
      int b = e.dst;
      for (int c : adj[b]) {
        if (c == a) {
          continue;
        }
        for (int d : adj[c]) {
          if (d != a && d != b && edgeSet.contains((long) d * 100000 + a)) {
            count++;
          }
        }
      }
    }
    return (int) Math.min(count, Integer.MAX_VALUE);
  }

  /** Count diamonds: two triangles sharing edge a→b.
   *  Pattern: a→b, b→c, c→a, b→d, d→a (c≠d). */
  private int countDiamonds(List<GraphEdge> edges) {
    Set<Long> edgeSet = new HashSet<>();
    int maxNode = 0;
    for (GraphEdge e : edges) {
      edgeSet.add((long) e.src * 100000 + e.dst);
      maxNode = Math.max(maxNode, Math.max(e.src, e.dst));
    }
    @SuppressWarnings("unchecked")
    List<Integer>[] adj = new List[maxNode + 1];
    for (int i = 0; i <= maxNode; i++) {
      adj[i] = new ArrayList<>();
    }
    for (GraphEdge e : edges) {
      adj[e.src].add(e.dst);
    }
    long count = 0;
    // For each edge a→b, count pairs (c,d) where b→c, c→a, b→d, d→a, c≠d
    for (GraphEdge e : edges) {
      int a = e.src;
      int b = e.dst;
      // Find all nodes reachable from b that close back to a
      List<Integer> closers = new ArrayList<>();
      for (int x : adj[b]) {
        if (x != a && x != b && edgeSet.contains((long) x * 100000 + a)) {
          closers.add(x);
        }
      }
      // Each pair (c, d) from closers with c≠d is one diamond
      count += (long) closers.size() * (closers.size() - 1);
    }
    return (int) Math.min(count, Integer.MAX_VALUE);
  }

  /** Estimate intermediate result size of R(a,b) JOIN S(b,c) on b. */
  private long estimateIntermediateSize(List<GraphEdge> edges, int nodes) {
    int[] inDeg = new int[nodes];
    int[] outDeg = new int[nodes];
    for (GraphEdge e : edges) {
      if (e.dst < nodes) {
        inDeg[e.dst]++;
      }
      if (e.src < nodes) {
        outDeg[e.src]++;
      }
    }
    long total = 0;
    for (int b = 0; b < nodes; b++) {
      total += (long) inDeg[b] * outDeg[b];
    }
    return total;
  }

  // ---------------------------------------------------------------
  // Query generation
  // ---------------------------------------------------------------

  // --- Triangle: a→b→c→a (3 tables) ---
  private static final String TRIANGLE_FROM =
      "s.edges1 e1, s.edges2 e2, s.edges3 e3";
  private static final String TRIANGLE_WHERE =
      "e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

  // --- Rectangle (4-cycle): a→b→c→d→a (4 tables) ---
  private static final String RECTANGLE_FROM =
      "s.edges1 e1, s.edges2 e2, s.edges3 e3, s.edges4 e4";
  private static final String RECTANGLE_WHERE =
      "e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e4.src AND e4.dst = e1.src";

  // --- Diamond: two triangles sharing edge a→b (5 tables) ---
  // e1: a→b, e2: b→c, e3: c→a, e4: b→d, e5: d→a
  private static final String DIAMOND_FROM =
      "s.edges1 e1, s.edges2 e2, s.edges3 e3, s.edges4 e4, s.edges5 e5";
  private static final String DIAMOND_WHERE =
      "e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src"
          + " AND e1.dst = e4.src AND e4.dst = e5.src AND e5.dst = e1.src";

  /** Generate query variations for the given shape. */
  private List<String> generateQueries(String shape, int n) {
    List<String> queries = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      queries.add(queryVariation(shape, i));
    }
    return queries;
  }

  /** Generate a MULTI() query wrapping N variations of the given shape. */
  private String generateMultiQuery(String shape, int n) {
    StringBuilder sb = new StringBuilder();
    sb.append("MULTI(\n");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(",\n");
      }
      sb.append("(").append(queryVariation(shape, i)).append(")");
    }
    sb.append("\n)");
    return sb.toString();
  }

  /** Dispatch to shape-specific query variation. */
  private String queryVariation(String shape, int index) {
    switch (shape) {
    case "triangle":
      return triangleVariation(index);
    case "rectangle":
      return rectangleVariation(index);
    case "diamond":
      return diamondVariation(index);
    default:
      throw new IllegalArgumentException("Unknown shape: " + shape);
    }
  }

  /** Triangle query variations with different projections. */
  private String triangleVariation(int index) {
    switch (index % 5) {
    case 0:
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 1:
      return "SELECT e1.src AS s1, e2.src AS s2, e3.src AS s3 "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 2:
      return "SELECT e1.src AS a, e2.dst AS c, e3.weight AS w "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 3:
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c, e3.weight AS w "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    default:
      return "SELECT e3.dst AS x, e1.src AS y, e2.src AS z, e1.weight AS w "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    }
  }

  /** Rectangle (4-cycle) query variations with different projections. */
  private String rectangleVariation(int index) {
    switch (index % 5) {
    case 0:
      // All 4 cycle vertices
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c, e3.dst AS d "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    case 1:
      // Source columns from each table
      return "SELECT e1.src AS s1, e2.src AS s2, e3.src AS s3, e4.src AS s4 "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    case 2:
      // Partial projection with weight
      return "SELECT e1.src AS a, e2.dst AS c, e4.weight AS w "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    case 3:
      // Full with weight
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c, e3.dst AS d, e4.weight AS w "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    default:
      // Reversed perspective
      return "SELECT e4.dst AS x, e1.src AS y, e2.src AS z, e3.src AS q, e1.weight AS w "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    }
  }

  /** Diamond query variations with different projections. */
  private String diamondVariation(int index) {
    switch (index % 5) {
    case 0:
      // All 4 variables: a, b, c, d
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c, e4.dst AS d "
          + "FROM " + DIAMOND_FROM + " WHERE " + DIAMOND_WHERE;
    case 1:
      // Source columns
      return "SELECT e1.src AS s1, e2.src AS s2, e3.src AS s3, e4.src AS s4, e5.src AS s5 "
          + "FROM " + DIAMOND_FROM + " WHERE " + DIAMOND_WHERE;
    case 2:
      // Partial with weight
      return "SELECT e1.src AS a, e2.dst AS c, e4.dst AS d, e3.weight AS w "
          + "FROM " + DIAMOND_FROM + " WHERE " + DIAMOND_WHERE;
    case 3:
      // Full with weights from both closing edges
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c, e4.dst AS d, "
          + "e3.weight AS w1, e5.weight AS w2 "
          + "FROM " + DIAMOND_FROM + " WHERE " + DIAMOND_WHERE;
    default:
      // Reversed perspective
      return "SELECT e3.dst AS x, e1.src AS y, e2.src AS z, e4.src AS q, e1.weight AS w "
          + "FROM " + DIAMOND_FROM + " WHERE " + DIAMOND_WHERE;
    }
  }

  // ---------------------------------------------------------------
  // Execution modes
  // ---------------------------------------------------------------

  private boolean shouldRun(String mode) {
    return modeFilter == null || modeFilter.equals(mode);
  }

  private String[] getShapes() {
    if ("all".equals(shapeFilter)) {
      return ALL_SHAPES;
    }
    return new String[]{shapeFilter};
  }

  /** Baseline: standard binary hash joins. */
  private TimingResult executeBaseline(Connection conn, List<String> queries) throws Exception {
    int rowCount = 0;
    long startNs = System.nanoTime();
    for (String query : queries) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          rowCount++;
        }
      }
    }
    long endNs = System.nanoTime();
    return new TimingResult(endNs - startNs, rowCount);
  }

  /** WCOJ: adds WCOJ rules, lets optimizer choose. */
  private TimingResult executeWcoj(Connection conn, List<String> queries) throws Exception {
    Consumer<RelOptPlanner> hook = planner -> {
      planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
      planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
    };

    int rowCount = 0;
    long startNs = System.nanoTime();
    try (Hook.Closeable ignored = Hook.PLANNER.addThread(hook)) {
      for (String query : queries) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            rowCount++;
          }
        }
      }
    }
    long endNs = System.nanoTime();
    return new TimingResult(endNs - startNs, rowCount);
  }

  /** Combine: MULTI() with WCOJ rules. */
  private TimingResult executeCombine(Connection conn, String multiQuery) throws Exception {
    Consumer<RelOptPlanner> hook = planner -> {
      planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
      planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
    };

    int rowCount = 0;
    long startNs = System.nanoTime();
    try (Hook.Closeable ignored = Hook.PLANNER.addThread(hook);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(multiQuery)) {
      while (rs.next()) {
        rowCount++;
      }
    }
    long endNs = System.nanoTime();
    return new TimingResult(endNs - startNs, rowCount);
  }

  /** Combine-share: MULTI() with WCOJ + scan sharing + prefix sharing. */
  private TimingResult executeCombineShare(Connection conn, String multiQuery) throws Exception {
    Consumer<RelOptPlanner> hook = planner -> {
      planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
      planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
      planner.addRule(CoreRules.COMBINE_SHARED_COMPONENTS);
      planner.addRule(EnumerableRules.ENUMERABLE_COMBINE_WCOJ_PREFIX_RULE);
    };

    int rowCount = 0;
    long startNs = System.nanoTime();
    try (Hook.Closeable ignored = Hook.PLANNER.addThread(hook);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(multiQuery)) {
      while (rs.next()) {
        rowCount++;
      }
    }
    long endNs = System.nanoTime();
    return new TimingResult(endNs - startNs, rowCount);
  }

  // ---------------------------------------------------------------
  // Main run loop
  // ---------------------------------------------------------------

  private void run() throws Exception {
    if (!csvOutput) {
      printHeader();
    }

    GraphEdge[] graphEdges = generateDenseHubGraph(numNodes, numEdges, seed);
    GraphSchema schema = new GraphSchema(graphEdges);
    List<GraphEdge> edgeList = Arrays.asList(graphEdges);

    // Connect using programmatic schema
    Properties props = new Properties();
    props.setProperty("lex", "JAVA");

    // Collect all results keyed by "shape/mode"
    Map<String, List<TimingResult>> allResults = new LinkedHashMap<>();

    try (Connection rawConn = DriverManager.getConnection("jdbc:calcite:", props)) {
      CalciteConnection calciteConn = rawConn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      rootSchema.add("s", new ReflectiveSchemaWithoutRowCount(schema));

      for (String shape : getShapes()) {
        runShape(rawConn, shape, edgeList, allResults);
      }
    }

    if (csvOutput) {
      printCsvResults(allResults);
    } else {
      printResults(allResults);
    }
  }

  /** Run warmup + measurement for one query shape. */
  private void runShape(Connection conn, String shape, List<GraphEdge> edgeList,
      Map<String, List<TimingResult>> allResults) throws Exception {

    if (!csvOutput) {
      System.out.printf("%n--- Shape: %s ---%n", shape);
      printGraphStats(edgeList, shape);
    }

    List<String> singleQueries = generateQueries(shape, queryCount);
    String multiQuery = generateMultiQuery(shape, queryCount);

    // Warmup
    if (!csvOutput) {
      System.out.println("\n[Warmup Phase]");
    }
    for (int w = 0; w < warmupIterations; w++) {
      if (shouldRun("baseline")) {
        executeBaseline(conn, singleQueries);
      }
      if (shouldRun("wcoj")) {
        executeWcoj(conn, singleQueries);
      }
      if (shouldRun("combine")) {
        executeCombine(conn, multiQuery);
      }
      if (shouldRun("combine-share")) {
        executeCombineShare(conn, multiQuery);
      }
      if (!csvOutput) {
        System.out.printf("  Warmup %d/%d complete%n", w + 1, warmupIterations);
      }
    }

    System.gc();
    Thread.sleep(100);

    // Measurement
    String[] modes = {"baseline", "wcoj", "combine", "combine-share"};
    for (String mode : modes) {
      if (!shouldRun(mode)) {
        continue;
      }
      if (!csvOutput) {
        System.out.printf("%n[Measurement: %s / %s]%n", shape, mode);
      }

      List<TimingResult> results = new ArrayList<>();
      for (int i = 0; i < measureIterations; i++) {
        TimingResult result;
        switch (mode) {
        case "baseline":
          result = executeBaseline(conn, singleQueries);
          break;
        case "wcoj":
          result = executeWcoj(conn, singleQueries);
          break;
        case "combine":
          result = executeCombine(conn, multiQuery);
          break;
        case "combine-share":
          result = executeCombineShare(conn, multiQuery);
          break;
        default:
          throw new IllegalStateException("Unknown mode: " + mode);
        }
        results.add(result);
        if (verbose && !csvOutput) {
          System.out.printf("  Iter %2d: %,10d ns  (%,7.3f ms)  rows=%d%n",
              i + 1, result.totalTimeNs, result.totalTimeNs / 1_000_000.0,
              result.rowCount);
        }
      }
      allResults.put(shape + "/" + mode, results);

      System.gc();
      Thread.sleep(100);
    }
  }

  // ---------------------------------------------------------------
  // Output
  // ---------------------------------------------------------------

  private void printHeader() {
    System.out.println("=================================================================");
    System.out.println("       WCOJ Benchmark: Baseline vs WCOJ vs Multi-Query WCOJ");
    System.out.println("=================================================================");
    System.out.println();
    System.out.printf("Configuration:%n");
    System.out.printf("  Nodes:        %d%n", numNodes);
    System.out.printf("  Edges:        %d (target)%n", numEdges);
    System.out.printf("  Seed:         %d%n", seed);
    System.out.printf("  Queries:      %d (variations per shape)%n", queryCount);
    System.out.printf("  Shapes:       %s%n", shapeFilter);
    System.out.printf("  Warmup:       %d iterations%n", warmupIterations);
    System.out.printf("  Measurement:  %d iterations%n", measureIterations);
    if (modeFilter != null) {
      System.out.printf("  Mode filter:  %s%n", modeFilter);
    }
  }

  private void printResults(Map<String, List<TimingResult>> allResults) {
    System.out.println();
    System.out.println("=================================================================");
    System.out.println("                          RESULTS");
    System.out.println("=================================================================");

    Map<String, Stats> allStats = new LinkedHashMap<>();
    for (Map.Entry<String, List<TimingResult>> entry : allResults.entrySet()) {
      Stats stats = calculateStats(entry.getValue());
      allStats.put(entry.getKey(), stats);

      System.out.println();
      System.out.printf("[%s]%n", entry.getKey());
      System.out.printf("  Rows:      %d%n", entry.getValue().get(0).rowCount);
      printStats(stats);
    }

    // Per-shape comparisons
    for (String shape : getShapes()) {
      Stats baselineStats = allStats.get(shape + "/baseline");
      if (baselineStats == null) {
        continue;
      }

      System.out.println();
      System.out.println("=================================================================");
      System.out.printf("                   COMPARISONS (%s)%n", shape);
      System.out.println("=================================================================");

      for (String mode : new String[]{"wcoj", "combine", "combine-share"}) {
        Stats modeStats = allStats.get(shape + "/" + mode);
        if (modeStats != null) {
          printComparison("baseline", baselineStats, mode, modeStats);
        }
      }

      Stats wcojStats = allStats.get(shape + "/wcoj");
      Stats combineStats = allStats.get(shape + "/combine");
      Stats combineShareStats = allStats.get(shape + "/combine-share");

      if (wcojStats != null && combineStats != null) {
        printComparison("wcoj", wcojStats, "combine", combineStats);
      }
      if (combineStats != null && combineShareStats != null) {
        printComparison("combine", combineStats, "combine-share", combineShareStats);
      }
    }

    System.out.println("=================================================================");
  }

  private void printComparison(String nameA, Stats a, String nameB, Stats b) {
    double diffNs = a.mean - b.mean;
    double speedup = b.mean > 0 ? a.mean / b.mean : 0;
    double improvement = a.mean > 0 ? (diffNs / a.mean) * 100 : 0;

    System.out.println();
    System.out.printf("  %s vs %s:%n", nameA, nameB);
    System.out.printf("    %-15s mean: %,12.3f ms%n", nameA, a.mean / 1_000_000.0);
    System.out.printf("    %-15s mean: %,12.3f ms%n", nameB, b.mean / 1_000_000.0);
    System.out.printf("    Speedup:     %12.2fx%n", speedup);
    System.out.printf("    Improvement: %11.1f%%%n", improvement);
  }

  private void printStats(Stats stats) {
    System.out.printf("  Mean:      %,15.3f ms%n", stats.mean / 1_000_000.0);
    System.out.printf("  Median:    %,15.3f ms%n", stats.median / 1_000_000.0);
    System.out.printf("  Std Dev:   %,15.3f ms%n", stats.stdDev / 1_000_000.0);
    System.out.printf("  Min:       %,15.3f ms%n", stats.min / 1_000_000.0);
    System.out.printf("  Max:       %,15.3f ms%n", stats.max / 1_000_000.0);
  }

  private void printCsvResults(Map<String, List<TimingResult>> allResults) {
    System.out.println("shape,mode,iteration,time_ns,time_ms,rows");
    for (Map.Entry<String, List<TimingResult>> entry : allResults.entrySet()) {
      // Key format: "shape/mode"
      String[] parts = entry.getKey().split("/", 2);
      String shape = parts[0];
      String mode = parts[1];
      List<TimingResult> results = entry.getValue();
      for (int i = 0; i < results.size(); i++) {
        TimingResult r = results.get(i);
        System.out.printf("%s,%s,%d,%d,%.3f,%d%n",
            shape, mode, i + 1, r.totalTimeNs, r.totalTimeNs / 1_000_000.0,
            r.rowCount);
      }
    }
  }

  // ---------------------------------------------------------------
  // Statistics
  // ---------------------------------------------------------------

  private Stats calculateStats(List<TimingResult> results) {
    List<Long> times = new ArrayList<>();
    for (TimingResult r : results) {
      times.add(r.totalTimeNs);
    }
    Collections.sort(times);

    double mean = times.stream().mapToLong(Long::longValue).average().orElse(0);
    double variance = times.stream()
        .mapToDouble(t -> Math.pow(t - mean, 2))
        .average().orElse(0);
    double stdDev = Math.sqrt(variance);

    return new Stats(
        mean,
        times.get(times.size() / 2),
        stdDev,
        times.get(0),
        times.get(times.size() - 1)
    );
  }

  // ---------------------------------------------------------------
  // Data classes
  // ---------------------------------------------------------------

  static class TimingResult {
    final long totalTimeNs;
    final int rowCount;

    TimingResult(long totalTimeNs, int rowCount) {
      this.totalTimeNs = totalTimeNs;
      this.rowCount = rowCount;
    }
  }

  static class Stats {
    final double mean;
    final double median;
    final double stdDev;
    final double min;
    final double max;

    Stats(double mean, double median, double stdDev, double min, double max) {
      this.mean = mean;
      this.median = median;
      this.stdDev = stdDev;
      this.min = min;
      this.max = max;
    }
  }
}
