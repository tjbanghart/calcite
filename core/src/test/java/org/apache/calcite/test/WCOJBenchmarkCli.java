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
 * on cyclic (triangle) queries over a dense graph dataset.
 *
 * <p>WCOJ wins over binary joins when intermediate results explode.
 * This benchmark constructs "dense hub" graphs where hub nodes create
 * O(N^2) intermediate pairs in binary join R(a,b) JOIN S(b,c), but
 * far fewer actual triangles exist. WCOJ avoids this blowup via
 * variable-at-a-time intersection.
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
 *   --queries=N      Triangle query variations for multi modes (default: 5)
 *   --warmup=N       Warmup iterations (default: 3)
 *   --iterations=N   Measurement iterations (default: 10)
 *   --mode=MODE      Only run: baseline|wcoj|combine|combine-share
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
  private long seed = 42;
  private boolean csvOutput = false;
  private boolean verbose = true;

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
    System.out.println("  --queries=N      Triangle query variations for multi modes (default: 5)");
    System.out.println("  --warmup=N       Warmup iterations (default: 3)");
    System.out.println("  --iterations=N   Measurement iterations (default: 10)");
    System.out.println("  --mode=MODE      Only run: baseline|wcoj|combine|combine-share");
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

  /** Schema with three edge tables for triangle queries.
   *  All three point to the same edge data (self-join over one graph).
   *  Separate table names avoid column-naming issues in self-join rewrites. */
  public static class GraphSchema {
    public final GraphEdge[] edges1;
    public final GraphEdge[] edges2;
    public final GraphEdge[] edges3;

    GraphSchema(GraphEdge[] edges) {
      this.edges1 = edges;
      this.edges2 = edges;
      this.edges3 = edges;
    }
  }

  /**
   * Generate a "dense hub" graph designed to stress binary joins.
   *
   * <p>Creates hub nodes that connect to many spokes. Binary join
   * R(a,b) JOIN S(b,c) on b=hub produces O(fanIn * fanOut) intermediate
   * rows per hub, but only a fraction close the triangle (c→a).
   *
   * <p>This is the worst case for binary joins: huge intermediates,
   * small output. WCOJ avoids the blowup by intersecting candidates
   * variable-at-a-time.
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

    // Add some spoke-to-spoke edges to create triangles
    // hub→spoke1, spoke1→spoke2, spoke2→hub forms a triangle
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

    if (!csvOutput) {
      // Count triangles for reporting
      int triangles = countTriangles(edgeList);
      System.out.printf("  Graph:        %d edges, %d hubs, ~%d triangles%n",
          edgeList.size(), numHubs, triangles);
      // Estimate binary join intermediate size
      long intermediateEst = estimateIntermediateSize(edgeList, nodes);
      System.out.printf("  Binary join intermediate est: ~%,d rows%n", intermediateEst);
    }

    return edgeList.toArray(new GraphEdge[0]);
  }

  /** Count actual triangles in the edge list (for verification). */
  private int countTriangles(List<GraphEdge> edges) {
    Set<Long> edgeSet = new HashSet<>();
    int maxNode = 0;
    for (GraphEdge e : edges) {
      edgeSet.add((long) e.src * 100000 + e.dst);
      maxNode = Math.max(maxNode, Math.max(e.src, e.dst));
    }
    // Build adjacency for a→b
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
      // For triangle a→b→c→a, check all c reachable from b
      for (int c : adj[b]) {
        if (edgeSet.contains((long) c * 100000 + a)) {
          count++;
        }
      }
    }
    return count;
  }

  /** Estimate intermediate result size of R(a,b) JOIN S(b,c) on b. */
  private long estimateIntermediateSize(List<GraphEdge> edges, int nodes) {
    // Count in-degree and out-degree per node
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
    // For each node b, binary join produces inDeg[b] * outDeg[b] rows
    long total = 0;
    for (int b = 0; b < nodes; b++) {
      total += (long) inDeg[b] * outDeg[b];
    }
    return total;
  }

  // ---------------------------------------------------------------
  // Query generation — matches EnumerableWCOJTest patterns exactly
  // ---------------------------------------------------------------

  /** Triangle query: find all a→b→c→a.
   *  Uses comma-join + WHERE (same syntax as EnumerableWCOJTest). */
  private static final String TRIANGLE_FROM =
      "s.edges1 e1, s.edges2 e2, s.edges3 e3";
  private static final String TRIANGLE_WHERE =
      "e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

  /** Generate N triangle query variations wrapped in MULTI(). */
  private String generateMultiTriangleQuery(int n) {
    StringBuilder sb = new StringBuilder();
    sb.append("MULTI(\n");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(",\n");
      }
      sb.append("(").append(triangleVariation(i)).append(")");
    }
    sb.append("\n)");
    return sb.toString();
  }

  /** Triangle query variations with different projections.
   *  Uses explicit aliases to avoid column-name collisions in MULTI(). */
  private String triangleVariation(int index) {
    switch (index % 5) {
    case 0:
      // Triangle vertices: a→b→c→a
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 1:
      // All source columns from each table
      return "SELECT e1.src AS s1, e2.src AS s2, e3.src AS s3 "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 2:
      // Triangle with weight from closing edge
      return "SELECT e1.src AS a, e2.dst AS c, e3.weight AS w "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 3:
      // Full triangle with weight
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c, e3.weight AS w "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    default:
      // Reversed perspective: c→a→b with weight
      return "SELECT e3.dst AS x, e1.src AS y, e2.src AS z, e1.weight AS w "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    }
  }

  // ---------------------------------------------------------------
  // Execution modes
  // ---------------------------------------------------------------

  private boolean shouldRun(String mode) {
    return modeFilter == null || modeFilter.equals(mode);
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

    // Single triangle query (for baseline and wcoj sequential modes)
    List<String> singleQueries = new ArrayList<>();
    for (int i = 0; i < queryCount; i++) {
      singleQueries.add(triangleVariation(i));
    }

    // MULTI() query (for combine modes)
    String multiQuery = generateMultiTriangleQuery(queryCount);

    // Connect using programmatic schema
    Properties props = new Properties();
    props.setProperty("lex", "JAVA");

    Map<String, List<TimingResult>> allResults = new LinkedHashMap<>();

    try (Connection rawConn = DriverManager.getConnection("jdbc:calcite:", props)) {
      CalciteConnection calciteConn = rawConn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      rootSchema.add("s", new ReflectiveSchemaWithoutRowCount(schema));

      // Warmup
      if (!csvOutput) {
        System.out.println("\n[Warmup Phase]");
      }
      for (int w = 0; w < warmupIterations; w++) {
        if (shouldRun("baseline")) {
          executeBaseline(rawConn, singleQueries);
        }
        if (shouldRun("wcoj")) {
          executeWcoj(rawConn, singleQueries);
        }
        if (shouldRun("combine")) {
          executeCombine(rawConn, multiQuery);
        }
        if (shouldRun("combine-share")) {
          executeCombineShare(rawConn, multiQuery);
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
          System.out.printf("%n[Measurement: %s]%n", mode);
        }

        List<TimingResult> results = new ArrayList<>();
        for (int i = 0; i < measureIterations; i++) {
          TimingResult result;
          switch (mode) {
          case "baseline":
            result = executeBaseline(rawConn, singleQueries);
            break;
          case "wcoj":
            result = executeWcoj(rawConn, singleQueries);
            break;
          case "combine":
            result = executeCombine(rawConn, multiQuery);
            break;
          case "combine-share":
            result = executeCombineShare(rawConn, multiQuery);
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
        allResults.put(mode, results);

        System.gc();
        Thread.sleep(100);
      }
    }

    if (csvOutput) {
      printCsvResults(allResults);
    } else {
      printResults(allResults);
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
    System.out.printf("  Queries:      %d (triangle variations)%n", queryCount);
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

    if (allStats.size() > 1) {
      System.out.println();
      System.out.println("=================================================================");
      System.out.println("                        COMPARISONS");
      System.out.println("=================================================================");

      Stats baselineStats = allStats.get("baseline");
      for (Map.Entry<String, Stats> entry : allStats.entrySet()) {
        if (entry.getKey().equals("baseline") || baselineStats == null) {
          continue;
        }
        printComparison("baseline", baselineStats, entry.getKey(), entry.getValue());
      }

      Stats wcojStats = allStats.get("wcoj");
      Stats combineStats = allStats.get("combine");
      Stats combineShareStats = allStats.get("combine-share");

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
    System.out.println("mode,iteration,time_ns,time_ms,rows");
    for (Map.Entry<String, List<TimingResult>> entry : allResults.entrySet()) {
      List<TimingResult> results = entry.getValue();
      for (int i = 0; i < results.size(); i++) {
        TimingResult r = results.get(i);
        System.out.printf("%s,%d,%d,%.3f,%d%n",
            entry.getKey(), i + 1, r.totalTimeNs, r.totalTimeNs / 1_000_000.0,
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
