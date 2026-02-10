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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Hook;

import org.apache.calcite.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Command-line benchmark tool for comparing MULTI query execution
 * with and without shared component optimization.
 *
 * <p>Usage:
 * <pre>
 *   java -cp <classpath> org.apache.calcite.test.MultiQueryBenchmarkCli [options]
 *
 * Options:
 *   --queries=N       Number of queries (default: 50)
 *   --selectivity=N   Selectivity percentage 0-100 (default: 50)
 *   --warmup=N        Warmup iterations (default: 3)
 *   --iterations=N    Measurement iterations (default: 10)
 *   --scale=N         TPC-H scale factor (default: 0.01)
 *   --no-share        Only run without sharing
 *   --share-only      Only run with sharing
 *   --csv             Output in CSV format
 *   --verbose         Show per-iteration details
 * </pre>
 */
public class MultiQueryBenchmarkCli {

  // Configuration
  private int queryCount = 50;
  private int selectivity = 50;
  private int warmupIterations = 3;
  private int measureIterations = 10;
  private double scale = 0.01;
  private boolean runNoShare = true;
  private boolean runShare = true;
  private boolean csvOutput = false;
  private boolean verbose = true;

  public static void main(String[] args) throws Exception {
    // Register the Calcite JDBC driver
    DriverManager.registerDriver(new Driver());

    MultiQueryBenchmarkCli cli = new MultiQueryBenchmarkCli();
    cli.parseArgs(args);
    cli.run();
  }

  private void parseArgs(String[] args) {
    for (String arg : args) {
      if (arg.startsWith("--queries=")) {
        queryCount = Integer.parseInt(arg.substring("--queries=".length()));
      } else if (arg.startsWith("--selectivity=")) {
        selectivity = Integer.parseInt(arg.substring("--selectivity=".length()));
      } else if (arg.startsWith("--warmup=")) {
        warmupIterations = Integer.parseInt(arg.substring("--warmup=".length()));
      } else if (arg.startsWith("--iterations=")) {
        measureIterations = Integer.parseInt(arg.substring("--iterations=".length()));
      } else if (arg.startsWith("--scale=")) {
        scale = Double.parseDouble(arg.substring("--scale=".length()));
      } else if (arg.equals("--no-share")) {
        runShare = false;
      } else if (arg.equals("--share-only")) {
        runNoShare = false;
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
    System.out.println("MULTI Query Sharing Benchmark");
    System.out.println();
    System.out.println("Usage: java -cp <classpath> org.apache.calcite.test.MultiQueryBenchmarkCli [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --queries=N       Number of queries in MULTI (default: 50)");
    System.out.println("  --selectivity=N   Selectivity percentage 0-100 (default: 50)");
    System.out.println("  --warmup=N        Warmup iterations (default: 3)");
    System.out.println("  --iterations=N    Measurement iterations (default: 10)");
    System.out.println("  --scale=N         TPC-H scale factor (default: 0.01)");
    System.out.println("  --no-share        Only run without sharing optimization");
    System.out.println("  --share-only      Only run with sharing optimization");
    System.out.println("  --csv             Output results in CSV format");
    System.out.println("  --verbose         Show per-iteration timing details");
    System.out.println("  --help, -h        Show this help message");
  }

  private void run() throws Exception {
    String multiQuery = generateMultiQuery(queryCount, selectivity);

    if (!csvOutput) {
      printHeader();
    }

    // Create connection
    String model = String.format("{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TPCH',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      type: 'custom',\n"
        + "      name: 'TPCH',\n"
        + "      factory: 'org.apache.calcite.adapter.tpch.TpchSchemaFactory',\n"
        + "      operand: {\n"
        + "        columnPrefix: false,\n"
        + "        scale: %f\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}", scale);

    Properties props = new Properties();
    props.setProperty("model", "inline:" + model);

    List<TimingResult> noShareResults = null;
    List<TimingResult> shareResults = null;

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Warmup
      if (!csvOutput) {
        System.out.println("\n[Warmup Phase]");
      }
      for (int i = 0; i < warmupIterations; i++) {
        if (runNoShare) {
          executeQuery(conn, multiQuery, false);
        }
        if (runShare) {
          executeQuery(conn, multiQuery, true);
        }
        if (!csvOutput) {
          System.out.printf("  Warmup %d/%d complete%n", i + 1, warmupIterations);
        }
      }

      // Force GC before measurement
      System.gc();
      Thread.sleep(100);

      // Measurement - No Sharing
      if (runNoShare) {
        if (!csvOutput) {
          System.out.println("\n[Measurement: WITHOUT Sharing]");
        }
        noShareResults = new ArrayList<>();
        for (int i = 0; i < measureIterations; i++) {
          TimingResult result = executeQuery(conn, multiQuery, false);
          noShareResults.add(result);
          if (verbose && !csvOutput) {
            System.out.printf("  Iter %2d: %,10d ns  (%,7.3f ms)  rows=%d%n",
                i + 1, result.totalTimeNs, result.totalTimeNs / 1_000_000.0, result.rowCount);
          }
        }
      }

      // Force GC between modes
      System.gc();
      Thread.sleep(100);

      // Measurement - With Sharing
      if (runShare) {
        if (!csvOutput) {
          System.out.println("\n[Measurement: WITH Sharing]");
        }
        shareResults = new ArrayList<>();
        for (int i = 0; i < measureIterations; i++) {
          TimingResult result = executeQuery(conn, multiQuery, true);
          shareResults.add(result);
          if (verbose && !csvOutput) {
            System.out.printf("  Iter %2d: %,10d ns  (%,7.3f ms)  rows=%d%n",
                i + 1, result.totalTimeNs, result.totalTimeNs / 1_000_000.0, result.rowCount);
          }
        }
      }
    }

    // Print results
    if (csvOutput) {
      printCsvResults(noShareResults, shareResults);
    } else {
      printResults(noShareResults, shareResults);
    }
  }

  private void printHeader() {
    System.out.println("╔══════════════════════════════════════════════════════════════════╗");
    System.out.println("║           MULTI Query Sharing Performance Benchmark              ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.printf("Configuration:%n");
    System.out.printf("  Queries:      %d%n", queryCount);
    System.out.printf("  Selectivity:  %d%%%n", selectivity);
    System.out.printf("  Scale:        %.2f%n", scale);
    System.out.printf("  Warmup:       %d iterations%n", warmupIterations);
    System.out.printf("  Measurement:  %d iterations%n", measureIterations);
  }

  private TimingResult executeQuery(Connection conn, String query, boolean enableSharing)
      throws Exception {
    Consumer<RelOptPlanner> plannerHook = planner -> {
      if (enableSharing) {
        planner.addRule(CoreRules.COMBINE_SHARED_COMPONENTS);
      }
    };

    int rowCount = 0;
    long startNs = System.nanoTime();

    try (Hook.Closeable ignored = Hook.PLANNER.addThread(plannerHook);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        rowCount++;
      }
    }

    long endNs = System.nanoTime();
    return new TimingResult(endNs - startNs, rowCount);
  }

  private void printResults(List<TimingResult> noShare, List<TimingResult> share) {
    System.out.println();
    System.out.println("╔══════════════════════════════════════════════════════════════════╗");
    System.out.println("║                         RESULTS                                  ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════╝");

    if (noShare != null) {
      Stats noShareStats = calculateStats(noShare);
      System.out.println();
      System.out.println("WITHOUT Sharing:");
      printStats(noShareStats);
    }

    if (share != null) {
      Stats shareStats = calculateStats(share);
      System.out.println();
      System.out.println("WITH Sharing:");
      printStats(shareStats);
    }

    if (noShare != null && share != null) {
      Stats noShareStats = calculateStats(noShare);
      Stats shareStats = calculateStats(share);

      System.out.println();
      System.out.println("══════════════════════════════════════════════════════════════════");
      System.out.println("                        COMPARISON");
      System.out.println("══════════════════════════════════════════════════════════════════");

      double diffNs = noShareStats.mean - shareStats.mean;
      double speedup = shareStats.mean > 0 ? noShareStats.mean / shareStats.mean : 0;
      double improvement = noShareStats.mean > 0 ? (diffNs / noShareStats.mean) * 100 : 0;

      System.out.printf("  No Sharing Mean:   %,15.3f ms%n", noShareStats.mean / 1_000_000.0);
      System.out.printf("  With Sharing Mean: %,15.3f ms%n", shareStats.mean / 1_000_000.0);
      System.out.printf("  Difference:        %,15.3f ms%n", diffNs / 1_000_000.0);
      System.out.printf("  Speedup:           %15.2fx%n", speedup);
      System.out.printf("  Improvement:       %14.1f%%%n", improvement);
      System.out.println("══════════════════════════════════════════════════════════════════");
    }
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

  private void printCsvResults(List<TimingResult> noShare, List<TimingResult> share) {
    // Header
    System.out.println("mode,iteration,time_ns,time_ms,rows");

    if (noShare != null) {
      for (int i = 0; i < noShare.size(); i++) {
        TimingResult r = noShare.get(i);
        System.out.printf("no_share,%d,%d,%.3f,%d%n",
            i + 1, r.totalTimeNs, r.totalTimeNs / 1_000_000.0, r.rowCount);
      }
    }

    if (share != null) {
      for (int i = 0; i < share.size(); i++) {
        TimingResult r = share.get(i);
        System.out.printf("share,%d,%d,%.3f,%d%n",
            i + 1, r.totalTimeNs, r.totalTimeNs / 1_000_000.0, r.rowCount);
      }
    }
  }

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

  private String generateMultiQuery(int count, int selectivityPct) {
    StringBuilder sb = new StringBuilder();
    sb.append("MULTI(\n");

    int qtyThreshold = 1 + (int) ((50 - 1) * selectivityPct / 100.0);
    double priceThreshold = 900.0 + (105000.0 - 900.0) * selectivityPct / 100.0;

    String[] lineitemCols = {"l_orderkey", "l_partkey", "l_suppkey", "l_quantity",
        "l_extendedprice", "l_discount", "l_tax", "l_returnflag"};
    String[] ordersCols = {"o_orderkey", "o_custkey", "o_totalprice", "o_orderdate",
        "o_orderpriority", "o_orderstatus"};

    int group1 = count / 4;
    int group2 = count / 4;
    int group3 = count / 4;
    int group4 = count - group1 - group2 - group3;

    int q = 0;

    // Group 1: LINEITEM with quantity filter
    for (int i = 0; i < group1; i++) {
      if (q > 0) sb.append(",\n");
      String cols = selectCols(lineitemCols, i, 3);
      sb.append(String.format("(SELECT %s FROM lineitem WHERE l_quantity <= %d LIMIT 100)",
          cols, qtyThreshold));
      q++;
    }

    // Group 2: LINEITEM with price filter
    for (int i = 0; i < group2; i++) {
      sb.append(",\n");
      String cols = selectCols(lineitemCols, i, 3);
      sb.append(String.format("(SELECT %s FROM lineitem WHERE l_extendedprice <= %.2f LIMIT 100)",
          cols, priceThreshold));
      q++;
    }

    // Group 3: LINEITEM-ORDERS join
    for (int i = 0; i < group3; i++) {
      sb.append(",\n");
      String lCols = "l." + selectCols(lineitemCols, i, 2).replace(", ", ", l.");
      String oCols = "o." + selectCols(ordersCols, i, 2).replace(", ", ", o.");
      sb.append(String.format("(SELECT %s, %s FROM lineitem l, orders o "
              + "WHERE l.l_orderkey = o.o_orderkey AND l.l_quantity <= %d LIMIT 100)",
          lCols, oCols, qtyThreshold));
      q++;
    }

    // Group 4: Aggregates
    String[] aggs = {"sum(l_quantity)", "avg(l_quantity)", "count(*)", "max(l_extendedprice)", "min(l_discount)"};
    for (int i = 0; i < group4; i++) {
      sb.append(",\n");
      sb.append(String.format("(SELECT l_returnflag, %s as val FROM lineitem "
              + "WHERE l_quantity <= %d GROUP BY l_returnflag)",
          aggs[i % aggs.length], qtyThreshold));
      q++;
    }

    sb.append("\n)");
    return sb.toString();
  }

  private String selectCols(String[] cols, int variation, int count) {
    List<String> selected = new ArrayList<>();
    selected.add(cols[0]);
    for (int i = 0; i < count - 1 && i + 1 < cols.length; i++) {
      int idx = 1 + ((variation + i) % (cols.length - 1));
      selected.add(cols[idx]);
    }
    return String.join(", ", selected);
  }

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
