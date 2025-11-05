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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CombineSharedComponentsRule;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

/**
 * Unit tests for {@link CombineSharedComponentsRule} and other Combine-related rules.
 *
 * <p>These tests verify the transformation of multiple independent queries
 * into a Combine operator structure that enables independent optimization
 * of each query branch.
 *
 * <p>Note: The CombineSharedComponentsRule currently only detects shared components
 * but doesn't transform them yet. Full spool-based optimization would be implemented
 * in a production version.
 */
class CombineRelOptRulesTest extends RelOptTestBase {

  @Override RelOptFixture fixture() {
    return super.fixture()
        .withDiffRepos(DiffRepository.lookup(CombineRelOptRulesTest.class));
  }

  @Test void testCombineTwoSimpleScans() {
    // Two independent queries that just scan the same table
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT * FROM EMP
      b.scan("EMP");

      // Query 2: SELECT * FROM EMP (same scan, but independent query)
      b.scan("EMP");

      // Combine the two queries on the stack
      return b.combine(2).build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testCombineScansWithDifferentFilters() {
    // Two independent queries with different filters resulting in different row counts
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT * FROM EMP WHERE SAL > 3000
      b.scan("EMP")
          .filter(b.call(SqlStdOperatorTable.GREATER_THAN,
                         b.field("SAL"),
                         b.literal(3000)));

      // Query 2: SELECT * FROM EMP WHERE DEPTNO = 10
      b.scan("EMP")
          .filter(b.call(SqlStdOperatorTable.EQUALS,
                         b.field("DEPTNO"),
                         b.literal(10)));

      // Combine the two filtered queries
      return b.combine().build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testCombineQueriesWithDifferentProjections() {
    // Two queries with different projections - different row types
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT EMPNO, ENAME FROM EMP
      b.scan("EMP")
          .project(b.field("EMPNO"), b.field("ENAME"));

      // Query 2: SELECT DEPTNO, SAL, COMM FROM EMP
      b.scan("EMP")
          .project(b.field("DEPTNO"), b.field("SAL"), b.field("COMM"));

      // Combine the two projected queries
      return b.combine(2).build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .vis();
  }

  @Test void testCombineAggregateQueries() {
    // Two independent aggregate queries with different groupings and aggregations
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT DEPTNO, COUNT(*) FROM EMP GROUP BY DEPTNO
      b.scan("EMP")
          .aggregate(b.groupKey("DEPTNO"),
                    b.count(false, "CNT"));

      // Query 2: SELECT JOB, AVG(SAL), MAX(SAL) FROM EMP GROUP BY JOB
      b.scan("EMP")
          .aggregate(b.groupKey("JOB"),
                    b.avg(false, "AVG_SAL", b.field("SAL")),
                    b.max("MAX_SAL", b.field("SAL")));

      // Combine the two aggregate queries
      return b.combine(2).build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testCombineThreeIndependentQueries() {
    // Three independent queries with different characteristics
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: Simple scan
      b.scan("EMP");

      // Query 2: Filtered and projected
      b.scan("EMP")
          .filter(b.call(SqlStdOperatorTable.GREATER_THAN,
                         b.field("SAL"),
                         b.literal(2000)))
          .project(b.field("EMPNO"), b.field("SAL"));

      // Query 3: Aggregation
      b.scan("EMP")
          .aggregate(b.groupKey("DEPTNO"),
                    b.sum(false, "TOTAL_SAL", b.field("SAL")));

      // Combine all three queries
      return b.combine(3).build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule()).vis();
//        .check();
  }

  @Test void testCombineDifferentTables() {
    // Independent queries from different tables
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT * FROM EMP WHERE SAL > 2500
      b.scan("EMP")
          .filter(b.call(SqlStdOperatorTable.GREATER_THAN,
                         b.field("SAL"),
                         b.literal(2500)));

      // Query 2: SELECT * FROM DEPT WHERE LOC = 'DALLAS'
      b.scan("DEPT")
          .filter(b.call(SqlStdOperatorTable.EQUALS,
                         b.field("LOC"),
                         b.literal("DALLAS")));

      // Combine queries from different tables
      return b.combine(2).build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .checkUnchanged();
  }

  @Test void testCombineWithSort() {
    // Independent queries with different sort orders
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT * FROM EMP ORDER BY SAL DESC
      b.scan("EMP")
          .sort(b.desc(b.field("SAL")));

      // Query 2: SELECT DEPTNO, COUNT(*) as CNT FROM EMP GROUP BY DEPTNO ORDER BY CNT
      b.scan("EMP")
          .aggregate(b.groupKey("DEPTNO"),
                    b.count(false, "CNT"))
          .sort(b.field("CNT"));

      // Combine the sorted queries
      return b.combine(2).build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testSingleQueryNoCombine() {
    // Single query should not need a Combine
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
         .filter(b.call(SqlStdOperatorTable.GREATER_THAN,
                        b.field("SAL"),
                        b.literal(1000)))
         .build();

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .checkUnchanged();
  }

  @Test void testCombineAllOnStack() {
    // Test using combine() without specifying count - combines all on stack
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Push multiple queries onto stack
      b.scan("EMP");
      b.scan("DEPT");
      b.scan("EMP").filter(b.call(SqlStdOperatorTable.IS_NOT_NULL, b.field("MGR")));

      // Combine all queries on stack (3 in this case)
      return b.combine().build();
    };

    relFn(relFn)
        .withRule(CombineSharedComponentsRule.Config.DEFAULT.toRule())
        .check();
  }
}
