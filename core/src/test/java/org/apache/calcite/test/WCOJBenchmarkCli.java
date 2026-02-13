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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.Consumer;

/**
 * Command-line benchmark comparing baseline binary joins, WCOJ,
 * and multi-query WCOJ with sharing on cyclic (triangle) queries
 * over a random graph dataset.
 *
 * <p>Usage:
 * <pre>
 *   java -cp &lt;classpath&gt; org.apache.calcite.test.WCOJBenchmarkCli [options]
 *
 * Options:
 *   --nodes=N        Number of graph vertices (default: 200)
 *   --edges=N        Number of directed edges (default: 1000)
 *   --queries=N      Number of triangle query variations for multi-query modes (default: 5)
 *   --warmup=N       Warmup iterations (default: 3)
 *   --iterations=N   Measurement iterations (default: 10)
 *   --mode=MODE      Run only this mode: baseline|wcoj|combine|combine-share (default: all)
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
    System.out.println("Usage: java -cp <classpath> org.apache.calcite.test.WCOJBenchmarkCli [options]");
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
  // Graph schema
  // ---------------------------------------------------------------

  /** A single directed edge in the graph. */
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

  /**
   * Schema exposing three copies of the edges table ({@code edges1},
   * {@code edges2}, {@code edges3}) so the triangle query uses three
   * distinct table scans — required for WCOJ rule matching.
   */
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

  private GraphEdge[] generateGraph(int nodes, int edges, long graphSeed) {
    Random rng = new Random(graphSeed);
    GraphEdge[] result = new GraphEdge[edges];
    for (int i = 0; i < edges; i++) {
      int src = rng.nextInt(nodes);
      int dst = rng.nextInt(nodes);
      double weight = rng.nextDouble() * 100.0;
      result[i] = new GraphEdge(src, dst, weight);
    }
    return result;
  }

  // ---------------------------------------------------------------
  // Query generation
  // ---------------------------------------------------------------

  /** Triangle join using comma-join syntax (same as WCOJ tests). */
  private static final String TRIANGLE_FROM =
      "s.edges1 e1, s.edges2 e2, s.edges3 e3";

  private static final String TRIANGLE_WHERE =
      "e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src";

  /** Single triangle query — find all directed triangles. */
  private String generateTriangleQuery() {
    return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c\n"
        + "FROM " + TRIANGLE_FROM + "\n"
        + "WHERE " + TRIANGLE_WHERE;
  }

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

  /** Generate a triangle query variation based on index.
   *  All variations share the same triangle join pattern with different
   *  projections. Uses unique column aliases to avoid name conflicts. */
  private String triangleVariation(int index) {
    switch (index % 5) {
    case 0:
      // Triangle vertices (a->b->c->a)
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c\n"
          + "FROM " + TRIANGLE_FROM + "\n"
          + "WHERE " + TRIANGLE_WHERE;
    case 1:
      // Triangle with all source vertices
      return "SELECT e1.src AS a, e2.src AS b, e3.src AS c\n"
          + "FROM " + TRIANGLE_FROM + "\n"
          + "WHERE " + TRIANGLE_WHERE;
    case 2:
      // Triangle with weight from third edge
      return "SELECT e1.src AS a, e1.dst AS b, e3.weight AS w\n"
          + "FROM " + TRIANGLE_FROM + "\n"
          + "WHERE " + TRIANGLE_WHERE;
    case 3:
      // Full triangle with weight
      return "SELECT e1.src AS a, e1.dst AS b, e2.dst AS c, e3.weight AS w\n"
          + "FROM " + TRIANGLE_FROM + "\n"
          + "WHERE " + TRIANGLE_WHERE;
    default:
      // Triangle with all weights
      return "SELECT e1.weight AS w1, e2.weight AS w2, e3.weight AS w3\n"
          + "FROM " + TRIANGLE_FROM + "\n"
          + "WHERE " + TRIANGLE_WHERE;
    }
  }

  // ---------------------------------------------------------------
  // Execution modes
  // ---------------------------------------------------------------

  private boolean shouldRun(String mode) {
    return modeFilter == null || modeFilter.equals(mode);
  }

  /** Baseline: standard binary hash joins, single query executed sequentially. */
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

  /** WCOJ: single query with WCOJ rules, executed sequentially. */
  private TimingResult executeWcoj(Connection conn, List<String> queries) throws Exception {
    Consumer<RelOptPlanner> plannerHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
    };

    int rowCount = 0;
    long startNs = System.nanoTime();

    try (Hook.Closeable ignored = Hook.PLANNER.addThread(plannerHook)) {
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

  /** Combine: MULTI() with WCOJ rules + TrieCache sharing. */
  private TimingResult executeCombine(Connection conn, String multiQuery) throws Exception {
    Consumer<RelOptPlanner> plannerHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
    };

    int rowCount = 0;
    long startNs = System.nanoTime();

    try (Hook.Closeable ignored = Hook.PLANNER.addThread(plannerHook);
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
    Consumer<RelOptPlanner> plannerHook = planner -> {
      planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
      planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
      planner.addRule(CoreRules.COMBINE_SHARED_COMPONENTS);
      planner.addRule(EnumerableRules.ENUMERABLE_COMBINE_WCOJ_PREFIX_RULE);
    };

    int rowCount = 0;
    long startNs = System.nanoTime();

    try (Hook.Closeable ignored = Hook.PLANNER.addThread(plannerHook);
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
    GraphEdge[] graphEdges = generateGraph(numNodes, numEdges, seed);
    GraphSchema schema = new GraphSchema(graphEdges);

    // Build individual triangle queries (for baseline and wcoj modes)
    List<String> singleQueries = new ArrayList<>();
    for (int i = 0; i < queryCount; i++) {
      singleQueries.add(triangleVariation(i));
    }

    // Build MULTI() query (for combine modes)
    String multiQuery = generateMultiTriangleQuery(queryCount);

    if (!csvOutput) {
      printHeader();
    }

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

      // Force GC before measurement
      System.gc();
      Thread.sleep(100);

      // Measurement for each mode
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
                i + 1, result.totalTimeNs, result.totalTimeNs / 1_000_000.0, result.rowCount);
          }
        }
        allResults.put(mode, results);

        // GC between modes
        System.gc();
        Thread.sleep(100);
      }
    }

    // Print results
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
    System.out.printf("  Edges:        %d%n", numEdges);
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

    // Pairwise comparisons
    if (allStats.size() > 1) {
      System.out.println();
      System.out.println("=================================================================");
      System.out.println("                        COMPARISONS");
      System.out.println("=================================================================");

      Stats baselineStats = allStats.get("baseline");
      for (Map.Entry<String, Stats> entry : allStats.entrySet()) {
        if (entry.getKey().equals("baseline")) {
          continue;
        }
        if (baselineStats != null) {
          printComparison("baseline", baselineStats, entry.getKey(), entry.getValue());
        }
      }

      // Also compare wcoj vs combine modes if both present
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
    System.out.printf("    %s mean: %,15.3f ms%n", nameA, a.mean / 1_000_000.0);
    System.out.printf("    %s mean: %,15.3f ms%n", nameB, b.mean / 1_000_000.0);
    System.out.printf("    Difference:  %,15.3f ms%n", diffNs / 1_000_000.0);
    System.out.printf("    Speedup:     %15.2fx%n", speedup);
    System.out.printf("    Improvement: %14.1f%%%n", improvement);
  }

  private void printStats(Stats stats) {
    System.out.printf("  Mean:      %,15.3f ms%n", stats.mean / 1_000_000.0);
    System.out.printf("  Median:    %,15.3f ms%n", stats.median / 1_000_000.0);
    System.out.printf("  Std Dev:   %,15.3f ms%n", stats.stdDev / 1_000_000.0);
    System.out.printf("  Min:       %,15.3f ms%n", stats.min / 1_000_000.0);
    System.out.printf("  Max:       %,15.3f ms%n", stats.max / 1_000_000.0);
    System.out.printf("  P50:       %,15.3f ms%n", stats.p50 / 1_000_000.0);
    System.out.printf("  P90:       %,15.3f ms%n", stats.p90 / 1_000_000.0);
    System.out.printf("  P99:       %,15.3f ms%n", stats.p99 / 1_000_000.0);
  }

  private void printCsvResults(Map<String, List<TimingResult>> allResults) {
    System.out.println("mode,iteration,time_ns,time_ms,rows");
    for (Map.Entry<String, List<TimingResult>> entry : allResults.entrySet()) {
      List<TimingResult> results = entry.getValue();
      for (int i = 0; i < results.size(); i++) {
        TimingResult r = results.get(i);
        System.out.printf("%s,%d,%d,%.3f,%d%n",
            entry.getKey(), i + 1, r.totalTimeNs, r.totalTimeNs / 1_000_000.0, r.rowCount);
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
        times.get(times.size() - 1),
        percentile(times, 50),
        percentile(times, 90),
        percentile(times, 99)
    );
  }

  private double percentile(List<Long> sorted, int p) {
    int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
    return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
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
    final double p50;
    final double p90;
    final double p99;

    Stats(double mean, double median, double stdDev, double min, double max,
        double p50, double p90, double p99) {
      this.mean = mean;
      this.median = median;
      this.stdDev = stdDev;
      this.min = min;
      this.max = max;
      this.p50 = p50;
      this.p90 = p90;
      this.p99 = p99;
    }
  }
}
