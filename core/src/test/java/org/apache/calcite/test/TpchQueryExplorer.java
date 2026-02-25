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
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Quick exploration tool to test candidate cyclic queries on TPC-H
 * and compare baseline vs WCOJ row counts and timing.
 */
public class TpchQueryExplorer {

  public static void main(String[] args) throws Exception {
    DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());

    double scale = 0.01;
    for (String arg : args) {
      if (arg.startsWith("--scale=")) {
        scale = Double.parseDouble(arg.substring("--scale=".length()));
      }
    }

    String model = String.format("{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TPCH',\n"
        + "  schemas: [{\n"
        + "    type: 'custom', name: 'TPCH',\n"
        + "    factory: 'org.apache.calcite.adapter.tpch.TpchSchemaFactory',\n"
        + "    operand: { columnPrefix: false, scale: %f }\n"
        + "  }]\n"
        + "}", scale);

    Properties props = new Properties();
    props.setProperty("model", "inline:" + model);

    // Candidate queries — name, SQL, expected cyclicity
    String[][] queries = {
        {"Q1: lineitem triangle (order-supp-part)",
            "SELECT l1.l_orderkey, l2.l_suppkey, l3.l_partkey "
                + "FROM lineitem l1, lineitem l2, lineitem l3 "
                + "WHERE l1.l_orderkey = l2.l_orderkey "
                + "AND l2.l_suppkey = l3.l_suppkey "
                + "AND l3.l_partkey = l1.l_partkey"},
        {"Q2: lineitem triangle (supp-order-part)",
            "SELECT l1.l_suppkey, l2.l_orderkey, l3.l_partkey "
                + "FROM lineitem l1, lineitem l2, lineitem l3 "
                + "WHERE l1.l_suppkey = l2.l_suppkey "
                + "AND l2.l_orderkey = l3.l_orderkey "
                + "AND l3.l_partkey = l1.l_partkey"},
        {"Q3: partsupp-lineitem-lineitem triangle",
            "SELECT ps.ps_partkey, l1.l_orderkey, l2.l_suppkey "
                + "FROM partsupp ps, lineitem l1, lineitem l2 "
                + "WHERE ps.ps_partkey = l1.l_partkey "
                + "AND ps.ps_suppkey = l2.l_suppkey "
                + "AND l1.l_orderkey = l2.l_orderkey"},
        {"Q4: same-customer orders sharing part (4-cycle)",
            "SELECT o1.o_custkey, l1.l_partkey, o2.o_orderkey, l2.l_quantity "
                + "FROM orders o1, lineitem l1, lineitem l2, orders o2 "
                + "WHERE o1.o_orderkey = l1.l_orderkey "
                + "AND l1.l_partkey = l2.l_partkey "
                + "AND l2.l_orderkey = o2.o_orderkey "
                + "AND o1.o_custkey = o2.o_custkey"},
        {"Q5: lineitem 4-cycle (supp-order-part-order)",
            "SELECT l1.l_orderkey, l2.l_suppkey, l3.l_partkey, l4.l_orderkey AS ok4 "
                + "FROM lineitem l1, lineitem l2, lineitem l3, lineitem l4 "
                + "WHERE l1.l_suppkey = l2.l_suppkey "
                + "AND l2.l_orderkey = l3.l_orderkey "
                + "AND l3.l_partkey = l4.l_partkey "
                + "AND l4.l_orderkey = l1.l_orderkey"},
    };

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      System.out.printf("TPC-H Query Explorer (SF=%.2f)%n", scale);
      System.out.println("=".repeat(80));

      for (String[] q : queries) {
        String name = q[0];
        String sql = q[1];
        System.out.printf("%n%s%n", name);
        System.out.printf("  SQL: %.100s...%n", sql);

        // Baseline
        try {
          long t0 = System.nanoTime();
          int rows = runQuery(conn, sql, false);
          long dt = System.nanoTime() - t0;
          System.out.printf("  Baseline: %,d rows in %,.1f ms%n", rows, dt / 1e6);
        } catch (Throwable e) {
          System.out.printf("  Baseline: ERROR - %s%n", e.getClass().getSimpleName()
              + ": " + e.getMessage());
          System.gc();
        }

        // WCOJ
        try {
          long t0 = System.nanoTime();
          int rows = runQuery(conn, sql, true);
          long dt = System.nanoTime() - t0;
          System.out.printf("  WCOJ:     %,d rows in %,.1f ms%n", rows, dt / 1e6);
        } catch (Throwable e) {
          System.out.printf("  WCOJ:     ERROR - %s%n", e.getClass().getSimpleName()
              + ": " + e.getMessage());
          System.gc();
        }
      }
    }
  }

  private static int runQuery(Connection conn, String sql, boolean wcoj) throws Exception {
    Consumer<RelOptPlanner> hook = planner -> {
      if (wcoj) {
        planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
        planner.addRule(EnumerableRules.ENUMERABLE_WCOJ_RULE);
      }
    };

    int rowCount = 0;
    try (Hook.Closeable ignored = Hook.PLANNER.addThread(hook);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        rowCount++;
      }
    }
    return rowCount;
  }
}
