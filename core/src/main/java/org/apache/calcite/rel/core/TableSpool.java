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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Spool that writes into a table.
 *
 * <p>NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
public abstract class TableSpool extends Spool {

  protected final RelOptTable table;

  protected TableSpool(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, Type readType, Type writeType, RelOptTable table) {
    super(cluster, traitSet, input, readType, writeType);
    this.table = requireNonNull(table, "table");
  }

  @Override public RelOptTable getTable() {
    return table;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    return pw.item("table", table.getQualifiedName());
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);

    // TableSpool has minimal self-cost - the input does the real work
    // Just charge for writing to temporary storage
    return planner.getCostFactory().makeCost(
        0,                    // rows: no additional row processing
        rowCount * 0.01,      // cpu: tiny management overhead
        rowCount * 0.05       // io: writing to temp (cheap sequential write)
    );
  }
}
