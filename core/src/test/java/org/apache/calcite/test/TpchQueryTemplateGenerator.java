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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Generator for TPC-H query templates designed to create shareable subexpressions.
 *
 * <p>This generator creates queries that share common subexpressions:
 * <ul>
 *   <li>Multiple queries share the same table scan</li>
 *   <li>Multiple queries share the same filtered scan (identical filter)</li>
 *   <li>Multiple queries share the same join pattern</li>
 *   <li>Only final projections/aggregations differ</li>
 * </ul>
 *
 * <p>This design ensures the CombineSharedComponentsRule can identify and
 * optimize shared components by introducing spools.
 *
 * <p>Scales to any number of queries by generating variations programmatically.
 */
public class TpchQueryTemplateGenerator {

  /** Selectivity levels from 0% to 90% in 10% increments. */
  private static final int[] SELECTIVITY_PERCENTAGES = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};

  // TPC-H date range: 1992-01-01 to 1998-12-31 (7 years = 2557 days)
  private static final String BASE_DATE = "1992-01-01";
  private static final int TOTAL_DAYS = 2557;

  // Value ranges for predicates
  private static final int MIN_QUANTITY = 1;
  private static final int MAX_QUANTITY = 50;
  private static final double MIN_PRICE = 900.0;
  private static final double MAX_PRICE = 105000.0;
  private static final double MIN_ORDER_PRICE = 800.0;
  private static final double MAX_ORDER_PRICE = 600000.0;
  private static final double MIN_DISCOUNT = 0.0;
  private static final double MAX_DISCOUNT = 0.10;

  // LINEITEM columns for projection variations
  private static final String[] LINEITEM_COLS = {
      "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
      "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
      "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode"
  };

  // ORDERS columns for projection variations
  private static final String[] ORDERS_COLS = {
      "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
      "o_orderpriority", "o_clerk", "o_shippriority"
  };

  // CUSTOMER columns for projection variations
  private static final String[] CUSTOMER_COLS = {
      "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone",
      "c_acctbal", "c_mktsegment"
  };

  // PART columns
  private static final String[] PART_COLS = {
      "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type",
      "p_size", "p_container", "p_retailprice"
  };

  // SUPPLIER columns
  private static final String[] SUPPLIER_COLS = {
      "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal"
  };

  // Aggregation functions
  private static final String[] AGG_FUNCTIONS = {"sum", "avg", "count", "max", "min"};

  // Numeric columns suitable for aggregation
  private static final String[] LINEITEM_NUMERIC = {
      "l_quantity", "l_extendedprice", "l_discount", "l_tax"
  };

  private static final String[] ORDERS_NUMERIC = {"o_totalprice", "o_shippriority"};

  private final int selectivityPercent;

  public TpchQueryTemplateGenerator(int selectivityPercent) {
    this.selectivityPercent = selectivityPercent;
  }

  /**
   * Main method to generate test files.
   */
  public static void main(String[] args) throws IOException {
    Path outputDir = Paths.get("core/src/test/resources/sql");
    if (args.length > 0) {
      outputDir = Paths.get(args[0]);
    }

    int selectivity = 50; // Default 50% selectivity
    if (args.length > 1) {
      selectivity = Integer.parseInt(args[1]);
    }

    int queryCount = 100; // Default 100 queries
    if (args.length > 2) {
      queryCount = Integer.parseInt(args[2]);
    }

    generateTestFiles(outputDir, selectivity, queryCount);
  }

  /**
   * Generates both shared and no-shared test files.
   */
  public static void generateTestFiles(Path outputDir, int selectivityPercent, int queryCount)
      throws IOException {
    Files.createDirectories(outputDir);

    TpchQueryTemplateGenerator generator = new TpchQueryTemplateGenerator(selectivityPercent);
    List<QueryTemplate> templates = generator.generateShareableTemplates(queryCount);

    // Generate shared file
    Path sharedFile = outputDir.resolve(
        String.format("generated-tpch-q%d-sel%d-shared.iq", queryCount, selectivityPercent));
    generator.writeIqFile(sharedFile, templates, true);
    System.out.println("Generated: " + sharedFile);

    // Generate no-shared file
    Path noSharedFile = outputDir.resolve(
        String.format("generated-tpch-q%d-sel%d-no-shared.iq", queryCount, selectivityPercent));
    generator.writeIqFile(noSharedFile, templates, false);
    System.out.println("Generated: " + noSharedFile);
  }

  /**
   * Generates all selectivity levels (0%, 10%, ..., 90%).
   */
  public static void generateAllSelectivityLevels(Path outputDir, int queryCount)
      throws IOException {
    for (int selectivity : SELECTIVITY_PERCENTAGES) {
      generateTestFiles(outputDir, selectivity, queryCount);
    }
  }

  /**
   * Generates query templates designed to share common subexpressions.
   * Scales to any requested count by generating variations programmatically.
   */
  public List<QueryTemplate> generateShareableTemplates(int count) {
    List<QueryTemplate> templates = new ArrayList<>();

    // Calculate thresholds based on selectivity
    int qtyThreshold = quantityThreshold();
    double priceThreshold = extendedPriceThreshold();
    double orderPriceThreshold = orderPriceThreshold();
    String dateThreshold = orderDateThreshold();
    double discountThreshold = discountThreshold();

    // Define sharing groups with their base expressions
    List<SharingGroup> groups = new ArrayList<>();

    // Group 1: LINEITEM with l_quantity filter
    groups.add(new SharingGroup(
        "LI-Qty",
        QueryCategory.SHARED_SCAN,
        "lineitem",
        String.format("l_quantity <= %d", qtyThreshold),
        LINEITEM_COLS,
        null, null, null));

    // Group 2: LINEITEM with l_extendedprice filter
    groups.add(new SharingGroup(
        "LI-Price",
        QueryCategory.SHARED_SCAN,
        "lineitem",
        String.format("l_extendedprice <= %.2f", priceThreshold),
        LINEITEM_COLS,
        null, null, null));

    // Group 3: LINEITEM with l_discount filter
    groups.add(new SharingGroup(
        "LI-Disc",
        QueryCategory.SHARED_SCAN,
        "lineitem",
        String.format("l_discount <= %.2f", discountThreshold),
        LINEITEM_COLS,
        null, null, null));

    // Group 4: Raw LINEITEM scan (no filter)
    groups.add(new SharingGroup(
        "LI-Raw",
        QueryCategory.SHARED_SCAN,
        "lineitem",
        null,
        LINEITEM_COLS,
        null, null, null));

    // Group 5: ORDERS with o_totalprice filter
    groups.add(new SharingGroup(
        "ORD-Price",
        QueryCategory.SHARED_SCAN,
        "orders",
        String.format("o_totalprice <= %.2f", orderPriceThreshold),
        ORDERS_COLS,
        null, null, null));

    // Group 6: ORDERS with o_orderdate filter
    groups.add(new SharingGroup(
        "ORD-Date",
        QueryCategory.SHARED_SCAN,
        "orders",
        String.format("o_orderdate <= date '%s'", dateThreshold),
        ORDERS_COLS,
        null, null, null));

    // Group 7: Raw ORDERS scan
    groups.add(new SharingGroup(
        "ORD-Raw",
        QueryCategory.SHARED_SCAN,
        "orders",
        null,
        ORDERS_COLS,
        null, null, null));

    // Group 8: LINEITEM-ORDERS join with quantity filter
    groups.add(new SharingGroup(
        "Join-LO-Qty",
        QueryCategory.SHARED_JOIN,
        "lineitem l, orders o",
        String.format("l.l_orderkey = o.o_orderkey and l.l_quantity <= %d", qtyThreshold),
        prefixColumns("l", LINEITEM_COLS),
        prefixColumns("o", ORDERS_COLS),
        null, null));

    // Group 9: LINEITEM-ORDERS join with price filter
    groups.add(new SharingGroup(
        "Join-LO-Price",
        QueryCategory.SHARED_JOIN,
        "lineitem l, orders o",
        String.format("l.l_orderkey = o.o_orderkey and l.l_extendedprice <= %.2f", priceThreshold),
        prefixColumns("l", LINEITEM_COLS),
        prefixColumns("o", ORDERS_COLS),
        null, null));

    // Group 10: CUSTOMER-ORDERS join with price filter
    groups.add(new SharingGroup(
        "Join-CO-Price",
        QueryCategory.SHARED_JOIN,
        "customer c, orders o",
        String.format("c.c_custkey = o.o_custkey and o.o_totalprice <= %.2f", orderPriceThreshold),
        prefixColumns("c", CUSTOMER_COLS),
        prefixColumns("o", ORDERS_COLS),
        null, null));

    // Group 11: PART-LINEITEM join
    groups.add(new SharingGroup(
        "Join-PL",
        QueryCategory.SHARED_JOIN,
        "part p, lineitem l",
        String.format("p.p_partkey = l.l_partkey and l.l_quantity <= %d", qtyThreshold),
        prefixColumns("p", PART_COLS),
        prefixColumns("l", LINEITEM_COLS),
        null, null));

    // Group 12: LINEITEM aggregates by returnflag (qty filter)
    groups.add(new SharingGroup(
        "Agg-LI-Flag",
        QueryCategory.SHARED_AGGREGATE,
        "lineitem",
        String.format("l_quantity <= %d", qtyThreshold),
        new String[]{"l_returnflag"},
        null,
        "l_returnflag",
        LINEITEM_NUMERIC));

    // Group 13: LINEITEM aggregates by shipmode (price filter)
    groups.add(new SharingGroup(
        "Agg-LI-Mode",
        QueryCategory.SHARED_AGGREGATE,
        "lineitem",
        String.format("l_extendedprice <= %.2f", priceThreshold),
        new String[]{"l_shipmode"},
        null,
        "l_shipmode",
        LINEITEM_NUMERIC));

    // Group 14: ORDERS aggregates by priority
    groups.add(new SharingGroup(
        "Agg-ORD-Pri",
        QueryCategory.SHARED_AGGREGATE,
        "orders",
        String.format("o_totalprice <= %.2f", orderPriceThreshold),
        new String[]{"o_orderpriority"},
        null,
        "o_orderpriority",
        ORDERS_NUMERIC));

    // Group 15: ORDERS aggregates by status
    groups.add(new SharingGroup(
        "Agg-ORD-Stat",
        QueryCategory.SHARED_AGGREGATE,
        "orders",
        String.format("o_orderdate <= date '%s'", dateThreshold),
        new String[]{"o_orderstatus"},
        null,
        "o_orderstatus",
        ORDERS_NUMERIC));

    // Group 16: CUSTOMER-ORDERS join aggregate by segment
    groups.add(new SharingGroup(
        "JoinAgg-CO-Seg",
        QueryCategory.SHARED_JOIN_AGGREGATE,
        "customer c, orders o",
        String.format("c.c_custkey = o.o_custkey and o.o_totalprice <= %.2f", orderPriceThreshold),
        new String[]{"c.c_mktsegment"},
        null,
        "c.c_mktsegment",
        prefixColumns("o", ORDERS_NUMERIC)));

    // Group 17: LINEITEM-ORDERS join aggregate by priority
    groups.add(new SharingGroup(
        "JoinAgg-LO-Pri",
        QueryCategory.SHARED_JOIN_AGGREGATE,
        "lineitem l, orders o",
        String.format("l.l_orderkey = o.o_orderkey and l.l_quantity <= %d", qtyThreshold),
        new String[]{"o.o_orderpriority"},
        null,
        "o.o_orderpriority",
        prefixColumns("l", LINEITEM_NUMERIC)));

    // Group 18: LINEITEM-ORDERS join aggregate by status
    groups.add(new SharingGroup(
        "JoinAgg-LO-Stat",
        QueryCategory.SHARED_JOIN_AGGREGATE,
        "lineitem l, orders o",
        String.format("l.l_orderkey = o.o_orderkey and l.l_extendedprice <= %.2f", priceThreshold),
        new String[]{"o.o_orderstatus"},
        null,
        "o.o_orderstatus",
        prefixColumns("l", LINEITEM_NUMERIC)));

    // Distribute queries across groups
    int queriesPerGroup = count / groups.size();
    int remainder = count % groups.size();

    int queryNum = 1;
    for (int g = 0; g < groups.size(); g++) {
      SharingGroup group = groups.get(g);
      int numQueries = queriesPerGroup + (g < remainder ? 1 : 0);

      List<QueryTemplate> groupQueries = generateGroupQueries(group, numQueries, queryNum);
      templates.addAll(groupQueries);
      queryNum += groupQueries.size();
    }

    return templates;
  }

  /**
   * Generates queries for a sharing group.
   */
  private List<QueryTemplate> generateGroupQueries(SharingGroup group, int count, int startNum) {
    List<QueryTemplate> queries = new ArrayList<>();

    switch (group.category) {
    case SHARED_SCAN:
    case SHARED_JOIN:
      // Generate projection variations
      for (int i = 0; i < count; i++) {
        String projection = generateProjection(group, i);
        String sql = buildSelectQuery(projection, group.fromClause, group.whereClause);
        queries.add(new QueryTemplate(
            String.format("%s-P%d", group.name, i + 1),
            group.category,
            sql));
      }
      break;

    case SHARED_AGGREGATE:
    case SHARED_JOIN_AGGREGATE:
      // Generate aggregation variations
      for (int i = 0; i < count; i++) {
        String aggExpr = generateAggregation(group, i);
        String sql = buildAggregateQuery(group.groupByKey, aggExpr, group.fromClause,
            group.whereClause, group.groupByKey);
        queries.add(new QueryTemplate(
            String.format("%s-A%d", group.name, i + 1),
            group.category,
            sql));
      }
      break;
    }

    return queries;
  }

  /**
   * Generates a projection clause with varying columns.
   */
  private String generateProjection(SharingGroup group, int variation) {
    List<String> allCols = new ArrayList<>();
    if (group.columns1 != null) {
      allCols.addAll(Arrays.asList(group.columns1));
    }
    if (group.columns2 != null) {
      allCols.addAll(Arrays.asList(group.columns2));
    }

    // Always include the first column (usually the key) plus 2-3 varying columns
    List<String> selected = new ArrayList<>();
    selected.add(allCols.get(0)); // Key column

    // Add varying columns based on variation number
    int numExtra = 2 + (variation % 2); // 2 or 3 extra columns
    for (int i = 0; i < numExtra; i++) {
      int idx = 1 + ((variation + i) % (allCols.size() - 1));
      if (!selected.contains(allCols.get(idx))) {
        selected.add(allCols.get(idx));
      }
    }

    return String.join(", ", selected);
  }

  /**
   * Generates an aggregation expression with varying functions and columns.
   */
  private String generateAggregation(SharingGroup group, int variation) {
    String[] aggCols = group.aggColumns;
    if (aggCols == null || aggCols.length == 0) {
      return "count(*) as cnt";
    }

    // Generate 1-3 aggregations per query
    int numAggs = 1 + (variation % 3);
    List<String> aggs = new ArrayList<>();

    for (int i = 0; i < numAggs; i++) {
      String func = AGG_FUNCTIONS[(variation + i) % AGG_FUNCTIONS.length];
      String col = aggCols[(variation + i) % aggCols.length];

      if (func.equals("count")) {
        aggs.add(String.format("count(*) as cnt_%d", i + 1));
      } else {
        String alias = String.format("%s_%s", func, col.replace(".", "_").replace("l_", "").replace("o_", ""));
        aggs.add(String.format("%s(%s) as %s", func, col, alias));
      }
    }

    return String.join(", ", aggs);
  }

  /**
   * Builds a SELECT query.
   */
  private String buildSelectQuery(String projection, String fromClause, String whereClause) {
    StringBuilder sb = new StringBuilder();
    sb.append("select ").append(projection).append("\n");
    sb.append("from ").append(fromClause).append("\n");
    if (whereClause != null && !whereClause.isEmpty()) {
      sb.append("where ").append(whereClause).append("\n");
    }
    sb.append("order by 1\n");
    sb.append("limit 100");
    return sb.toString();
  }

  /**
   * Builds an aggregate query.
   */
  private String buildAggregateQuery(String groupByCol, String aggExpr, String fromClause,
      String whereClause, String orderByCol) {
    StringBuilder sb = new StringBuilder();
    sb.append("select ").append(groupByCol).append(", ").append(aggExpr).append("\n");
    sb.append("from ").append(fromClause).append("\n");
    if (whereClause != null && !whereClause.isEmpty()) {
      sb.append("where ").append(whereClause).append("\n");
    }
    sb.append("group by ").append(groupByCol).append("\n");
    sb.append("order by ").append(orderByCol);
    return sb.toString();
  }

  /**
   * Prefixes column names with an alias.
   */
  private static String[] prefixColumns(String alias, String[] cols) {
    String[] result = new String[cols.length];
    for (int i = 0; i < cols.length; i++) {
      result[i] = alias + "." + cols[i];
    }
    return result;
  }

  // ============================================================================
  // Threshold calculation methods based on selectivity
  // ============================================================================

  private int quantityThreshold() {
    return MIN_QUANTITY + (int) ((MAX_QUANTITY - MIN_QUANTITY) * selectivityPercent / 100.0);
  }

  private double extendedPriceThreshold() {
    return MIN_PRICE + (MAX_PRICE - MIN_PRICE) * selectivityPercent / 100.0;
  }

  private double orderPriceThreshold() {
    return MIN_ORDER_PRICE + (MAX_ORDER_PRICE - MIN_ORDER_PRICE) * selectivityPercent / 100.0;
  }

  private String orderDateThreshold() {
    int days = (int) (TOTAL_DAYS * selectivityPercent / 100.0);
    java.time.LocalDate base = java.time.LocalDate.parse(BASE_DATE);
    java.time.LocalDate threshold = base.plusDays(days);
    return threshold.toString();
  }

  private double discountThreshold() {
    return MIN_DISCOUNT + (MAX_DISCOUNT - MIN_DISCOUNT) * selectivityPercent / 100.0;
  }

  // ============================================================================
  // File generation
  // ============================================================================

  private void writeIqFile(Path path, List<QueryTemplate> templates, boolean enableSharing)
      throws IOException {
    try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(path))) {
      writeHeader(writer, templates, enableSharing);
      writeAllQueriesInSingleMulti(writer, templates);
      writeFooter(writer, enableSharing);
    }
  }

  private void writeHeader(PrintWriter writer, List<QueryTemplate> templates,
      boolean enableSharing) {
    String suffix = enableSharing ? "shared" : "no-shared";
    writer.printf("# generated-tpch-q%d-sel%d-%s.iq - Generated TPC-H shareable query templates%n",
        templates.size(), selectivityPercent, suffix);
    writer.println("#");
    writer.println("# Licensed to the Apache Software Foundation (ASF) under one or more");
    writer.println("# contributor license agreements.  See the NOTICE file distributed with");
    writer.println("# this work for additional information regarding copyright ownership.");
    writer.println("# The ASF licenses this file to you under the Apache License, Version 2.0");
    writer.println("# (the \"License\"); you may not use this file except in compliance with");
    writer.println("# the License.  You may obtain a copy of the License at");
    writer.println("#");
    writer.println("# http://www.apache.org/licenses/LICENSE-2.0");
    writer.println("#");
    writer.println("# Unless required by applicable law or agreed to in writing, software");
    writer.println("# distributed under the License is distributed on an \"AS IS\" BASIS,");
    writer.println("# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
    writer.println("# See the License for the specific language governing permissions and");
    writer.println("# limitations under the License.");
    writer.println("#");
    writer.printf("# Query count: %d%n", templates.size());
    writer.printf("# Selectivity: %d%%%n", selectivityPercent);
    writer.printf("# Shared components: %s%n", enableSharing ? "ENABLED" : "DISABLED");
    writer.println("#");
    writer.println("# SHAREABLE QUERY DESIGN:");
    writer.println("# - Groups of queries share identical subexpressions (same filter/join)");
    writer.println("# - Only final projections or aggregations differ");
    writer.println("# - CombineSharedComponentsRule should detect and optimize these");
    writer.println("#");
    writer.println("!use tpch");
    writer.println("!set outputformat mysql");
    writer.println();
    writer.println("# Skip all HepPlanner preprocessing (including calc conversion)");
    writer.println("# and go directly to VolcanoPlanner for optimization");
    writer.println("!set volcanoonly true");
    writer.println();
    if (enableSharing) {
      writer.println("!set planner-rules \"+COMBINE_SHARED_COMPONENTS\"");
      writer.println();
    }
  }

  private void writeAllQueriesInSingleMulti(PrintWriter writer, List<QueryTemplate> templates) {
    // Count by category
    int sharedScan = 0, sharedJoin = 0, sharedAgg = 0, sharedJoinAgg = 0;
    for (QueryTemplate t : templates) {
      switch (t.category) {
      case SHARED_SCAN:
        sharedScan++;
        break;
      case SHARED_JOIN:
        sharedJoin++;
        break;
      case SHARED_AGGREGATE:
        sharedAgg++;
        break;
      case SHARED_JOIN_AGGREGATE:
        sharedJoinAgg++;
        break;
      }
    }

    writer.println("# ==============================================================================");
    writer.printf("# ALL QUERIES (%d total) - Designed for subexpression sharing%n", templates.size());
    writer.println("# Query breakdown:");
    writer.printf("#   - Shared scan queries: %d (groups share identical filtered scans)%n", sharedScan);
    writer.printf("#   - Shared join queries: %d (share identical join, differ in projection)%n", sharedJoin);
    writer.printf("#   - Shared aggregate queries: %d (share base, differ in agg function)%n", sharedAgg);
    writer.printf("#   - Shared join-aggregate queries: %d (share join+groupby, differ in agg)%n", sharedJoinAgg);
    writer.println("# ==============================================================================");
    writer.println();

    writer.println("MULTI(");

    for (int i = 0; i < templates.size(); i++) {
      QueryTemplate t = templates.get(i);
      writer.printf("-- Q%d: %s (%s) --%n", i + 1, t.name, categoryLabel(t.category));
      writer.print("(");
      writer.print(t.sql);
      writer.print(")");
      if (i < templates.size() - 1) {
        writer.println(",");
        writer.println();
      }
    }

    writer.println();
    writer.println(");");
    writer.println("!plan");
    writer.println("!ok");
    writer.println();
  }

  private String categoryLabel(QueryCategory category) {
    switch (category) {
    case SHARED_SCAN:
      return "SharedScan";
    case SHARED_JOIN:
      return "SharedJoin";
    case SHARED_AGGREGATE:
      return "SharedAgg";
    case SHARED_JOIN_AGGREGATE:
      return "SharedJoinAgg";
    default:
      return "Unknown";
    }
  }

  private void writeFooter(PrintWriter writer, boolean enableSharing) {
    String suffix = enableSharing ? "shared" : "no-shared";
    writer.printf("# End generated file%n");
  }

  // ============================================================================
  // Helper classes
  // ============================================================================

  /** Query category for shareable expressions. */
  enum QueryCategory {
    SHARED_SCAN,
    SHARED_JOIN,
    SHARED_AGGREGATE,
    SHARED_JOIN_AGGREGATE
  }

  /** Query template with name, category, and SQL. */
  static class QueryTemplate {
    final String name;
    final QueryCategory category;
    final String sql;

    QueryTemplate(String name, QueryCategory category, String sql) {
      this.name = name;
      this.category = category;
      this.sql = sql;
    }
  }

  /** Defines a group of queries that share a common subexpression. */
  static class SharingGroup {
    final String name;
    final QueryCategory category;
    final String fromClause;
    final String whereClause;
    final String[] columns1;      // Primary columns for projection
    final String[] columns2;      // Secondary columns (for joins)
    final String groupByKey;      // For aggregates
    final String[] aggColumns;    // Columns to aggregate

    SharingGroup(String name, QueryCategory category, String fromClause, String whereClause,
        String[] columns1, String[] columns2, String groupByKey, String[] aggColumns) {
      this.name = name;
      this.category = category;
      this.fromClause = fromClause;
      this.whereClause = whereClause;
      this.columns1 = columns1;
      this.columns2 = columns2;
      this.groupByKey = groupByKey;
      this.aggColumns = aggColumns;
    }
  }
}
