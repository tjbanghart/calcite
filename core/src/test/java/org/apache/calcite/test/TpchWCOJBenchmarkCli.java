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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Hook;

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
import java.util.function.Consumer;

/**
 * Command-line benchmark comparing baseline binary joins vs WCOJ
 * on cyclic queries over TPC-H data.
 *
 * <p>While standard TPC-H queries are acyclic, the schema naturally
 * supports cyclic join patterns — particularly lineitem self-joins
 * where the same table is joined on different keys:
 * <ul>
 *   <li><b>triangle</b>: lineitem self-join triangle
 *       (orderkey-suppkey-partkey cycle, 587K rows at SF=0.01)</li>
 *   <li><b>rectangle</b>: lineitem self-join 4-cycle
 *       (suppkey-orderkey-partkey-orderkey, 1.2M rows at SF=0.01)</li>
 *   <li><b>fk-triangle</b>: lineitem-partsupp-supplier foreign-key triangle
 *       (partkey-suppkey cycle, 301K rows at SF=0.01).
 *       <b>Note:</b> alpha-acyclic per GYO reduction; excluded from
 *       ALL_SHAPES. Use {@code --shape=fk-triangle --mode=baseline}.</li>
 *   <li><b>fk-rectangle</b>: customer-orders-lineitem-supplier 4-cycle
 *       (nationkey closes cycle, 11.7K rows at SF=0.01)</li>
 *   <li><b>fk-diamond</b>: customer-orders-lineitem-supplier-nation
 *       5-table cycle (nationkey routed through nation table,
 *       11.7K rows at SF=0.01). Called "diamond" by convention
 *       despite having 5 tables in the cycle.</li>
 * </ul>
 *
 * <p>Requires {@code -Dcalcite.enable.wcoj=true} JVM argument.
 *
 * <p>Usage:
 * <pre>
 *   ./gradlew :core:runTpchWcojBenchmark -PtpchWcojBenchmarkArgs="[options]"
 *
 * Options:
 *   --scale=N        TPC-H scale factor (default: 0.01)
 *   --shape=SHAPE    Query shape: triangle|rectangle|diamond|all (default: all)
 *   --queries=N      Query variations for multi modes (default: 5)
 *   --warmup=N       Warmup iterations (default: 10)
 *   --iterations=N   Measurement iterations (default: 10)
 *   --mode=MODE      Only run: baseline|wcoj|combine-binary|combine|combine-share
 *   --csv            Output in CSV format
 *   --verbose        Show per-iteration details
 * </pre>
 */
public class TpchWCOJBenchmarkCli {

  // Configuration
  private double scale = 0.01;
  private String shapeFilter = "all";
  private int queryCount = 5;
  private int warmupIterations = 10;
  private int measureIterations = 10;
  private String modeFilter = null;
  private boolean csvOutput = false;
  private boolean verbose = false;

  // fk-triangle is omitted: GYO reduction correctly identifies it as
  // alpha-acyclic (the suppkey equivalence class creates a ternary hyperedge
  // that witnesses the binary partkey hyperedge), so the WCOJ rule does not
  // fire. Running it in WCOJ mode would crash because binary join rules are
  // removed. Use --shape=fk-triangle with --mode=baseline to run it.
  private static final String[] ALL_SHAPES =
      {"triangle", "rectangle", "fk-rectangle", "fk-diamond"};

  public static void main(String[] args) throws Exception {
    boolean wcojEnabled =
        org.apache.calcite.config.CalciteSystemProperty.ENABLE_WCOJ.value();
    if (!wcojEnabled) {
      System.err.println("WARNING: calcite.enable.wcoj is not set. "
          + "WCOJ rules will not fire. Pass -Dcalcite.enable.wcoj=true");
    }

    DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());

    TpchWCOJBenchmarkCli cli = new TpchWCOJBenchmarkCli();
    cli.parseArgs(args);
    cli.run();
  }

  private void parseArgs(String[] args) {
    for (String arg : args) {
      if (arg.startsWith("--scale=")) {
        scale = Double.parseDouble(arg.substring("--scale=".length()));
      } else if (arg.startsWith("--shape=")) {
        shapeFilter = arg.substring("--shape=".length());
      } else if (arg.startsWith("--queries=")) {
        queryCount = Integer.parseInt(arg.substring("--queries=".length()));
      } else if (arg.startsWith("--warmup=")) {
        warmupIterations = Integer.parseInt(arg.substring("--warmup=".length()));
      } else if (arg.startsWith("--iterations=")) {
        measureIterations = Integer.parseInt(arg.substring("--iterations=".length()));
      } else if (arg.startsWith("--mode=")) {
        modeFilter = arg.substring("--mode=".length());
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
    System.out.println("TPC-H WCOJ Benchmark — Cyclic Joins on Realistic Data");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --scale=N        TPC-H scale factor (default: 0.01)");
    System.out.println("  --shape=SHAPE    Query shape: triangle|rectangle|fk-triangle|fk-rectangle|fk-diamond|all (default: all)");
    System.out.println("  --queries=N      Query variations for multi modes (default: 5)");
    System.out.println("  --warmup=N       Warmup iterations (default: 10)");
    System.out.println("  --iterations=N   Measurement iterations (default: 10)");
    System.out.println("  --mode=MODE      Only run: baseline|wcoj|combine-binary|combine|combine-share");
    System.out.println("  --csv            Output results in CSV format");
    System.out.println("  --verbose        Show per-iteration timing details");
    System.out.println("  --help, -h       Show this help message");
  }

  // ---------------------------------------------------------------
  // Query definitions — cyclic joins on TPC-H tables
  // ---------------------------------------------------------------

  // --- Triangle: lineitem self-join (orderkey-suppkey-partkey cycle) ---
  // l1-l2 share orderkey (items in same order)
  // l2-l3 share suppkey (items from same supplier)
  // l3-l1 share partkey (items for same part) → closes cycle
  // Binary join l1⋈l2 on orderkey produces ~4x fan-out per row,
  // then ⋈l3 on suppkey produces ~600x — massive intermediate explosion.
  private static final String TRIANGLE_FROM =
      "lineitem l1, lineitem l2, lineitem l3";
  private static final String TRIANGLE_WHERE =
      "l1.l_orderkey = l2.l_orderkey"
          + " AND l2.l_suppkey = l3.l_suppkey"
          + " AND l3.l_partkey = l1.l_partkey";

  // --- Rectangle: lineitem self-join 4-cycle (supp-order-part-order) ---
  // l1-l2 share suppkey, l2-l3 share orderkey,
  // l3-l4 share partkey, l4-l1 share orderkey → closes 4-cycle
  private static final String RECTANGLE_FROM =
      "lineitem l1, lineitem l2, lineitem l3, lineitem l4";
  private static final String RECTANGLE_WHERE =
      "l1.l_suppkey = l2.l_suppkey"
          + " AND l2.l_orderkey = l3.l_orderkey"
          + " AND l3.l_partkey = l4.l_partkey"
          + " AND l4.l_orderkey = l1.l_orderkey";

  // --- FK-Triangle: lineitem-partsupp-supplier (foreign key triangle) ---
  // l-ps on partkey, ps-s on suppkey, l-s on suppkey → closes cycle
  // More selective than self-joins; tests FK-based cyclicity
  private static final String FK_TRIANGLE_FROM =
      "lineitem l, partsupp ps, supplier s";
  private static final String FK_TRIANGLE_WHERE =
      "l.l_partkey = ps.ps_partkey"
          + " AND ps.ps_suppkey = s.s_suppkey"
          + " AND l.l_suppkey = s.s_suppkey";

  // --- FK-Rectangle: customer-orders-lineitem-supplier (nationkey cycle) ---
  // c-o on custkey, o-l on orderkey, l-s on suppkey,
  // s-c on nationkey → closes 4-cycle
  // Low-cardinality closing predicate (nationkey has 25 values)
  // lets binary joins prune early; WCOJ pays intersection cost regardless.
  private static final String FK_RECTANGLE_FROM =
      "customer c, orders o, lineitem l, supplier s";
  private static final String FK_RECTANGLE_WHERE =
      "c.c_custkey = o.o_custkey"
          + " AND o.o_orderkey = l.l_orderkey"
          + " AND l.l_suppkey = s.s_suppkey"
          + " AND s.s_nationkey = c.c_nationkey";

  // --- FK-Diamond: customer-orders-lineitem-supplier-nation (5-table cycle) ---
  // Same as FK-Rectangle but routes through the nation table,
  // adding a 5th table to the cycle: c-o-l-s-n-c
  // Even more WCOJ overhead from the extra intersection level.
  private static final String FK_DIAMOND_FROM =
      "customer c, orders o, lineitem l, supplier s, nation n";
  private static final String FK_DIAMOND_WHERE =
      "c.c_custkey = o.o_custkey"
          + " AND o.o_orderkey = l.l_orderkey"
          + " AND l.l_suppkey = s.s_suppkey"
          + " AND s.s_nationkey = n.n_nationkey"
          + " AND n.n_nationkey = c.c_nationkey";

  // ---------------------------------------------------------------
  // Query variations (5 per shape, different projections)
  // ---------------------------------------------------------------

  private String triangleVariation(int index) {
    switch (index % 5) {
    case 0:
      return "SELECT l1.l_orderkey, l2.l_suppkey, l3.l_partkey "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 1:
      return "SELECT l1.l_extendedprice, l2.l_quantity, l3.l_discount "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 2:
      return "SELECT l1.l_orderkey, l1.l_partkey, l2.l_suppkey, l3.l_shipdate "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    case 3:
      return "SELECT l2.l_orderkey, l2.l_suppkey, l3.l_partkey, l1.l_returnflag "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    default:
      return "SELECT l3.l_partkey, l1.l_quantity, l2.l_extendedprice, l3.l_tax "
          + "FROM " + TRIANGLE_FROM + " WHERE " + TRIANGLE_WHERE;
    }
  }

  private String rectangleVariation(int index) {
    switch (index % 5) {
    case 0:
      return "SELECT l1.l_orderkey, l2.l_suppkey, l3.l_partkey, l4.l_orderkey AS ok4 "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    case 1:
      return "SELECT l1.l_extendedprice, l2.l_quantity, l3.l_discount, l4.l_tax "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    case 2:
      return "SELECT l1.l_orderkey, l2.l_orderkey AS ok2, l3.l_partkey, l4.l_suppkey "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    case 3:
      return "SELECT l1.l_suppkey, l2.l_suppkey AS sk2, l3.l_orderkey AS ok3, l4.l_shipdate "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    default:
      return "SELECT l4.l_orderkey AS ok4, l1.l_quantity, l2.l_returnflag, l3.l_extendedprice "
          + "FROM " + RECTANGLE_FROM + " WHERE " + RECTANGLE_WHERE;
    }
  }

  private String fkTriangleVariation(int index) {
    switch (index % 5) {
    case 0:
      return "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey, ps.ps_availqty, s.s_name "
          + "FROM " + FK_TRIANGLE_FROM + " WHERE " + FK_TRIANGLE_WHERE;
    case 1:
      return "SELECT l.l_extendedprice, l.l_discount, ps.ps_supplycost, s.s_acctbal "
          + "FROM " + FK_TRIANGLE_FROM + " WHERE " + FK_TRIANGLE_WHERE;
    case 2:
      return "SELECT l.l_quantity, l.l_partkey, s.s_name, s.s_nationkey "
          + "FROM " + FK_TRIANGLE_FROM + " WHERE " + FK_TRIANGLE_WHERE;
    case 3:
      return "SELECT l.l_orderkey, ps.ps_partkey, ps.ps_suppkey, ps.ps_availqty, s.s_suppkey "
          + "FROM " + FK_TRIANGLE_FROM + " WHERE " + FK_TRIANGLE_WHERE;
    default:
      return "SELECT s.s_name, s.s_nationkey, l.l_shipdate, ps.ps_comment "
          + "FROM " + FK_TRIANGLE_FROM + " WHERE " + FK_TRIANGLE_WHERE;
    }
  }

  private String fkRectangleVariation(int index) {
    switch (index % 5) {
    case 0:
      return "SELECT c.c_name, o.o_orderdate, l.l_quantity, s.s_name "
          + "FROM " + FK_RECTANGLE_FROM + " WHERE " + FK_RECTANGLE_WHERE;
    case 1:
      return "SELECT c.c_custkey, o.o_totalprice, l.l_extendedprice, s.s_acctbal "
          + "FROM " + FK_RECTANGLE_FROM + " WHERE " + FK_RECTANGLE_WHERE;
    case 2:
      return "SELECT o.o_orderkey, l.l_partkey, l.l_suppkey, c.c_nationkey "
          + "FROM " + FK_RECTANGLE_FROM + " WHERE " + FK_RECTANGLE_WHERE;
    case 3:
      return "SELECT s.s_suppkey, l.l_discount, o.o_orderstatus, c.c_mktsegment "
          + "FROM " + FK_RECTANGLE_FROM + " WHERE " + FK_RECTANGLE_WHERE;
    default:
      return "SELECT c.c_name, l.l_shipdate, s.s_nationkey, o.o_orderpriority "
          + "FROM " + FK_RECTANGLE_FROM + " WHERE " + FK_RECTANGLE_WHERE;
    }
  }

  private String fkDiamondVariation(int index) {
    switch (index % 5) {
    case 0:
      return "SELECT c.c_name, o.o_orderdate, l.l_quantity, s.s_name, n.n_name "
          + "FROM " + FK_DIAMOND_FROM + " WHERE " + FK_DIAMOND_WHERE;
    case 1:
      return "SELECT c.c_custkey, o.o_totalprice, l.l_extendedprice, s.s_acctbal, n.n_regionkey "
          + "FROM " + FK_DIAMOND_FROM + " WHERE " + FK_DIAMOND_WHERE;
    case 2:
      return "SELECT o.o_orderkey, l.l_partkey, s.s_suppkey, n.n_name, c.c_nationkey "
          + "FROM " + FK_DIAMOND_FROM + " WHERE " + FK_DIAMOND_WHERE;
    case 3:
      return "SELECT s.s_name, l.l_discount, o.o_orderstatus, n.n_regionkey, c.c_mktsegment "
          + "FROM " + FK_DIAMOND_FROM + " WHERE " + FK_DIAMOND_WHERE;
    default:
      return "SELECT n.n_name, c.c_name, l.l_shipdate, s.s_nationkey, o.o_orderpriority "
          + "FROM " + FK_DIAMOND_FROM + " WHERE " + FK_DIAMOND_WHERE;
    }
  }

  private String queryVariation(String shape, int index) {
    switch (shape) {
    case "triangle":
      return triangleVariation(index);
    case "rectangle":
      return rectangleVariation(index);
    case "fk-triangle":
      return fkTriangleVariation(index);
    case "fk-rectangle":
      return fkRectangleVariation(index);
    case "fk-diamond":
      return fkDiamondVariation(index);
    default:
      throw new IllegalArgumentException("Unknown shape: " + shape);
    }
  }

  private List<String> generateQueries(String shape, int n) {
    List<String> queries = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      queries.add(queryVariation(shape, i));
    }
    return queries;
  }

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

  // ---------------------------------------------------------------
  // Execution modes (same pattern as WCOJBenchmarkCli)
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

  /** Combine-binary: MULTI() with standard binary hash joins (no WCOJ). */
  private TimingResult executeCombineBinary(Connection conn, String multiQuery) throws Exception {
    // No planner hook — uses default binary join rules
    int rowCount = 0;
    long startNs = System.nanoTime();
    try (Statement stmt = conn.createStatement();
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

    // TPC-H connection via model string
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

    Map<String, List<TimingResult>> allResults = new LinkedHashMap<>();

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      for (String shape : getShapes()) {
        runShape(conn, shape, allResults);
      }
    }

    if (csvOutput) {
      printCsvResults(allResults);
    } else {
      printResults(allResults);
    }
  }

  /** Run warmup + measurement for one query shape. */
  private void runShape(Connection conn, String shape,
      Map<String, List<TimingResult>> allResults) throws Exception {

    if (!csvOutput) {
      System.out.printf("%n--- Shape: %s ---%n", shape);
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
      if (shouldRun("combine-binary")) {
        executeCombineBinary(conn, multiQuery);
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
    String[] modes = {"baseline", "wcoj", "combine-binary", "combine", "combine-share"};
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
        case "combine-binary":
          result = executeCombineBinary(conn, multiQuery);
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
    System.out.println("  TPC-H WCOJ Benchmark: Cyclic Joins on Realistic Data");
    System.out.println("=================================================================");
    System.out.println();
    System.out.printf("Configuration:%n");
    System.out.printf("  Scale:        %.2f%n", scale);
    System.out.printf("  Shapes:       %s%n", shapeFilter);
    System.out.printf("  Queries:      %d (variations per shape)%n", queryCount);
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

      for (String mode : new String[]{"wcoj", "combine-binary", "combine", "combine-share"}) {
        Stats modeStats = allStats.get(shape + "/" + mode);
        if (modeStats != null) {
          printComparison("baseline", baselineStats, mode, modeStats);
        }
      }

      Stats wcojStats = allStats.get(shape + "/wcoj");
      Stats combineBinaryStats = allStats.get(shape + "/combine-binary");
      Stats combineStats = allStats.get(shape + "/combine");
      Stats combineShareStats = allStats.get(shape + "/combine-share");

      if (wcojStats != null && combineBinaryStats != null) {
        printComparison("wcoj", wcojStats, "combine-binary", combineBinaryStats);
      }
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
    // Sample standard deviation (dividing by N-1 for unbiased estimator)
    double sumSqDev = times.stream()
        .mapToDouble(t -> Math.pow(t - mean, 2))
        .sum();
    double stdDev = times.size() > 1
        ? Math.sqrt(sumSqDev / (times.size() - 1))
        : 0;

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
