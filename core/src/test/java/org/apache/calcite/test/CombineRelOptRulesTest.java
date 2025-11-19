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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CombineSharedComponentsRule;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

/**
 * Unit tests for {@link CombineSharedComponentsRule} demonstrating various
 * shared component patterns including joins, filters, aggregations, and projections.
 *
 * <p>These tests verify that the rule can identify and optimize shared
 * relational operations beyond simple table scans.
 */
class CombineRelOptRulesTest extends RelOptTestBase {

  @Override RelOptFixture fixture() {
    return super.fixture()
        .withDiffRepos(DiffRepository.lookup(CombineRelOptRulesTest.class));
  }

  // ========== Shared Join Tests ==========

  @Test void testSharedJoin() {
    // Two queries sharing the same EMP-DEPT join
    // Query 1: SELECT E.EMPNO, D.DNAME FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO WHERE E.SAL > 2000
    // Query 2: SELECT E.ENAME, D.LOC FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO WHERE D.LOC = 'CHICAGO'
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1
      b.scan("EMP")
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .filter(b.call(SqlStdOperatorTable.GREATER_THAN,
              b.field("SAL"),
              b.literal(2000)))
          .project(b.field("EMPNO"), b.field("DNAME"));

      // Query 2
      b.scan("EMP")
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .filter(b.call(SqlStdOperatorTable.EQUALS,
              b.field("LOC"),
              b.literal("CHICAGO")))
          .project(b.field("ENAME"), b.field("LOC"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedComplexJoin() {
    // Multiple queries sharing a 3-way join: EMP -> DEPT -> SALGRADE
    // Query 1: Count employees per department grade
    // Query 2: Average salary per department grade
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT D.DNAME, S.GRADE, COUNT(*) ...
      b.scan("EMP")
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .scan("SALGRADE")
          .join(b.call(SqlStdOperatorTable.AND,
              b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                  b.field(2, 0, "SAL"),
                  b.field(2, 1, "LOSAL")),
              b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                  b.field(2, 0, "SAL"),
                  b.field(2, 1, "HISAL"))))
          .aggregate(
              b.groupKey("DNAME", "GRADE"),
              b.count(false, "EMP_COUNT"));

      // Query 2: SELECT D.DNAME, S.GRADE, AVG(SAL) ...
      b.scan("EMP")
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .scan("SALGRADE")
          .join(b.call(SqlStdOperatorTable.AND,
              b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                  b.field(2, 0, "SAL"),
                  b.field(2, 1, "LOSAL")),
              b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                  b.field(2, 0, "SAL"),
                  b.field(2, 1, "HISAL"))))
          .aggregate(
              b.groupKey("DNAME", "GRADE"),
              b.avg(false, "AVG_SAL", b.field("SAL")));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  // ========== Shared Filter Tests ==========

  @Test void testSharedFilter() {
    // Two queries sharing the same filter condition
    // Query 1: SELECT EMPNO, SAL FROM EMP WHERE SAL > 2000 AND DEPTNO = 10
    // Query 2: SELECT ENAME, JOB FROM EMP WHERE SAL > 2000 AND DEPTNO = 10
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN,
                      b.field("SAL"),
                      b.literal(2000)),
                  b.call(SqlStdOperatorTable.EQUALS,
                      b.field("DEPTNO"),
                      b.literal(10))))
          .project(b.field("EMPNO"), b.field("SAL"));

      // Query 2
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN,
                      b.field("SAL"),
                      b.literal(2000)),
                  b.call(SqlStdOperatorTable.EQUALS,
                      b.field("DEPTNO"),
                      b.literal(10))))
          .project(b.field("ENAME"), b.field("JOB"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedFilterWithDifferentProjections() {
    // Three queries sharing same filter but different projections
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Shared filter: EMP WHERE SAL BETWEEN 1000 AND 3000
      // Query 1: Count
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(1000)),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(3000))))
          .aggregate(b.groupKey(), b.count(false, "CNT"));

      // Query 2: Average salary
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(1000)),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(3000))))
          .aggregate(b.groupKey(), b.avg(false, "AVG_SAL", b.field("SAL")));

      // Query 3: List of names
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.AND,
                  b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(1000)),
                  b.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                      b.field("SAL"),
                      b.literal(3000))))
          .project(b.field("ENAME"), b.field("SAL"));

      return b.combine(3).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  // ========== Shared Aggregation Tests ==========

  @Test void testSharedAggregationBase() {
    // Two queries that could share aggregation computation
    // Query 1: SELECT DEPTNO, SUM(SAL), COUNT(*) FROM EMP GROUP BY DEPTNO
    // Query 2: SELECT DEPTNO, SUM(SAL) FROM EMP GROUP BY DEPTNO WHERE SUM(SAL) > 10000
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: Basic aggregation
      b.scan("EMP")
          .aggregate(
              b.groupKey("DEPTNO"),
              b.sum(false, "TOTAL_SAL", b.field("SAL")),
              b.count(false, "EMP_CNT"));

      // Query 2: Same aggregation with HAVING clause
      b.scan("EMP")
          .aggregate(
              b.groupKey("DEPTNO"),
              b.sum(false, "TOTAL_SAL", b.field("SAL")))
          .filter(
              b.call(SqlStdOperatorTable.GREATER_THAN,
                  b.field("TOTAL_SAL"),
                  b.literal(10000)));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedJoinThenAggregation() {
    // Queries sharing join followed by different aggregations
    // Query 1: Total salary by department
    // Query 2: Employee count by location
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: SELECT D.DNAME, SUM(E.SAL) FROM EMP E JOIN DEPT D ... GROUP BY D.DNAME
      b.scan("EMP")
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("DNAME"),
              b.sum(false, "TOTAL_SAL", b.field("SAL")));

      // Query 2: SELECT D.LOC, COUNT(*) FROM EMP E JOIN DEPT D ... GROUP BY D.LOC
      b.scan("EMP")
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("LOC"),
              b.count(false, "EMP_CNT"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  // ========== Shared Projection Tests ==========

  @Test void testSharedProjection() {
    // Queries sharing same projection from base table
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: Filter on projected data
      b.scan("EMP")
          .project(
              b.field("EMPNO"),
              b.field("ENAME"),
              b.call(SqlStdOperatorTable.MULTIPLY,
                  b.field("SAL"),
                  b.literal(12)))  // Annual salary calculation
          .filter(
              b.call(SqlStdOperatorTable.GREATER_THAN,
                  b.field(2),
                  b.literal(50000)));

      // Query 2: Aggregate on same projected data
      b.scan("EMP")
          .project(
              b.field("EMPNO"),
              b.field("ENAME"),
              b.call(SqlStdOperatorTable.MULTIPLY,
                  b.field("SAL"),
                  b.literal(12)))
          .aggregate(
              b.groupKey(),
              b.avg(false, "AVG_ANNUAL", b.field(2)));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  // ========== Complex Shared Pattern Tests ==========

  @Test void testSharedFilterJoinAggregate() {
    // Complex pattern: Filter -> Join -> different aggregations
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Query 1: High earners by department with count
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.GREATER_THAN,
                  b.field("SAL"),
                  b.literal(2000)))
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("DNAME"),
              b.count(false, "HIGH_EARNER_CNT"));

      // Query 2: High earners by department with average
      b.scan("EMP")
          .filter(
              b.call(SqlStdOperatorTable.GREATER_THAN,
                  b.field("SAL"),
                  b.literal(2000)))
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .aggregate(
              b.groupKey("DNAME"),
              b.avg(false, "AVG_HIGH_SAL", b.field("SAL")));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedSubqueryPattern() {
    // Pattern that mimics: SELECT ... WHERE X IN (SELECT ... FROM shared_base)
    final Function<RelBuilder, RelNode> relFn = b -> {
      // Shared subquery base: Departments with high total salary
      // Query 1: Employees in high-salary departments
      b.scan("EMP")
          .scan("DEPT")
          .join(b.call(SqlStdOperatorTable.EQUALS,
              b.field(2, 0, "DEPTNO"),
              b.field(2, 1, "DEPTNO")))
          .project(
              b.field("EMPNO"),
              b.field("ENAME"),
              b.field("DNAME"));

      // Query 2: Department stats for high-salary departments
      b.scan("DEPT")
          .project(
              b.field("DEPTNO"),
              b.field("DNAME"),
              b.field("LOC"));

      return b.combine(2).build();
    };

    relFn(relFn)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  // ========== SQL-based Tests ==========

  @Test void testSharedJoinSQL() {
    String sql = "MULTI("
        + "(SELECT E.EMPNO, D.DNAME FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO WHERE E.SAL > 2000), "
        + "(SELECT E.ENAME, D.LOC FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO WHERE D.LOC = 'CHICAGO')"
        + ")";

    sql(sql)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedFilterSQL() {
    String sql = "MULTI("
        + "(SELECT EMPNO, SAL FROM EMP WHERE SAL > 2000 AND DEPTNO = 10), "
        + "(SELECT ENAME, JOB FROM EMP WHERE SAL > 2000 AND DEPTNO = 10)"
        + ")";

    sql(sql)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testSharedAggregationSQL() {
    String sql = "MULTI("
        + "(SELECT DEPTNO, SUM(SAL) as TOTAL, COUNT(*) as CNT FROM EMP GROUP BY DEPTNO), "
        + "(SELECT DEPTNO, AVG(SAL) as AVG_SAL FROM EMP GROUP BY DEPTNO)"
        + ")";

    sql(sql)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testComplexSharedPatternSQL() {
    // Filter -> Join -> Aggregate pattern
    String sql = "MULTI("
        + "(SELECT D.DNAME, COUNT(*) as CNT "
        + " FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO "
        + " WHERE E.SAL > 2000 GROUP BY D.DNAME), "
        + "(SELECT D.DNAME, AVG(E.SAL) as AVG_SAL "
        + " FROM EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO "
        + " WHERE E.SAL > 2000 GROUP BY D.DNAME)"
        + ")";

    sql(sql)
        .withVolcanoPlanner(false, planner -> {
          RelOptUtil.registerDefaultRules(planner, false, false);
          planner.addRule(CombineSharedComponentsRule.Config.DEFAULT.toRule());
        })
        .check();
  }
}
