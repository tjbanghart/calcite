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

import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CombineMultipleSpoolRule;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.AfterAll;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.plan.RelOptUtil.registerDefaultRules;

/**
 * Unit tests for {@link CombineMultipleSpoolRule} and other Combine-related rules.
 *
 * <p>These tests verify the transformation of queries with multiple CTEs
 * into a Combine operator structure that enables independent optimization
 * of each CTE.
 */
class CombineRelOptRulesTest extends RelOptTestBase {

  @Nullable
  private static DiffRepository diffRepos = null;

  @AfterAll
  public static void checkActualAndReferenceFiles() {
    if (diffRepos != null) {
      diffRepos.checkActualAndReferenceFiles();
    }
  }

  @Override RelOptFixture fixture() {
    RelOptFixture fixture = super.fixture()
        .withDiffRepos(DiffRepository.lookup(CombineRelOptRulesTest.class));
    diffRepos = fixture.diffRepos();
    return fixture;
  }


  @Test void testCombineMultipleCTEs() {
    final String sql = "WITH\n"
        + "  cte1 AS (SELECT deptno, COUNT(*) as cnt FROM emp GROUP BY deptno),\n"
        + "  cte2 AS (SELECT deptno, AVG(sal) as avg_sal FROM emp GROUP BY deptno)\n"
        + "SELECT c1.deptno, c1.cnt, c2.avg_sal\n"
        + "FROM cte1 c1\n"
        + "JOIN cte2 c2 ON c1.deptno = c2.deptno";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          registerDefaultRules(p, false, false);
          p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          // Add the rule we're testing (commented out since Combine doesn't exist yet)
           p.addRule(CombineMultipleSpoolRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testCombineThreeCTEs() {
    final String sql = "WITH\n"
        + "  dept_counts AS (SELECT deptno, COUNT(*) as cnt FROM emp GROUP BY deptno),\n"
        + "  dept_salaries AS (SELECT deptno, SUM(sal) as total_sal FROM emp GROUP BY deptno),\n"
        + "  dept_avg AS (SELECT deptno, AVG(sal) as avg_sal FROM emp GROUP BY deptno)\n"
        + "SELECT dc.deptno, dc.cnt, ds.total_sal, da.avg_sal\n"
        + "FROM dept_counts dc\n"
        + "JOIN dept_salaries ds ON dc.deptno = ds.deptno\n"
        + "JOIN dept_avg da ON dc.deptno = da.deptno";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          // p.addRule(CombineMultipleSpoolRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testCombineNestedCTEs() {
    final String sql = "WITH\n"
        + "  base_data AS (SELECT * FROM emp WHERE deptno IN (10, 20)),\n"
        + "  aggregated AS (SELECT deptno, COUNT(*) as cnt, AVG(sal) as avg_sal\n"
        + "                 FROM base_data\n"
        + "                 GROUP BY deptno)\n"
        + "SELECT * FROM aggregated WHERE cnt > 2";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(CombineMultipleSpoolRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testCombineCTEsWithUnion() {
    final String sql = "WITH\n"
        + "  high_sal AS (SELECT * FROM emp WHERE sal > 2000),\n"
        + "  low_sal AS (SELECT * FROM emp WHERE sal <= 2000)\n"
        + "SELECT * FROM high_sal\n"
        + "UNION ALL\n"
        + "SELECT * FROM low_sal";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(CombineMultipleSpoolRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testNoCombineForSingleCTE() {
    final String sql = "WITH\n"
        + "  dept_summary AS (SELECT deptno, COUNT(*) as cnt FROM emp GROUP BY deptno)\n"
        + "SELECT * FROM dept_summary WHERE cnt > 5";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(CombineMultipleSpoolRule.Config.DEFAULT.toRule());
        })
        .checkUnchanged();
  }

  @Test void testCombineCTEsReusedMultipleTimes() {
    final String sql = "WITH\n"
        + "  dept_stats AS (SELECT deptno, COUNT(*) as cnt, AVG(sal) as avg_sal\n"
        + "                 FROM emp GROUP BY deptno)\n"
        + "SELECT d1.deptno, d1.cnt, d2.avg_sal\n"
        + "FROM dept_stats d1\n"
        + "JOIN dept_stats d2 ON d1.deptno = d2.deptno\n"
        + "WHERE d1.cnt > 3";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(CombineMultipleSpoolRule.Config.DEFAULT.toRule());
        })
        .check();
  }

  @Test void testCombineComplexCTEHierarchy() {
    final String sql = "WITH\n"
        + "  base AS (SELECT * FROM emp WHERE deptno IS NOT NULL),\n"
        + "  dept_summary AS (SELECT deptno, COUNT(*) as emp_count FROM base GROUP BY deptno),\n"
        + "  high_earners AS (SELECT * FROM base WHERE sal > 3000),\n"
        + "  final AS (SELECT d.deptno, d.emp_count, COUNT(h.empno) as high_earner_count\n"
        + "            FROM dept_summary d\n"
        + "            LEFT JOIN high_earners h ON d.deptno = h.deptno\n"
        + "            GROUP BY d.deptno, d.emp_count)\n"
        + "SELECT * FROM final";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(CombineMultipleSpoolRule.Config.DEFAULT.toRule());
        })
        .check();
  }
}
