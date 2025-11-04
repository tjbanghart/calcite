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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.SpoolRelOptTable;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rule that optimizes a Combine operator by detecting shared components
 * (like common table scans) and introducing spools to avoid redundant computation.
 *
 * <p>For example, if a Combine has multiple queries that scan the same table,
 * this rule will introduce a spool so the table is scanned only once.
 */
@Value.Enclosing
public class CombineSharedComponentsRule extends RelRule<CombineSharedComponentsRule.Config> {

  /** Creates a CombineSharedComponentsRule. */
  protected CombineSharedComponentsRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Combine combine = call.rel(0);
    List<RelNode> inputs = combine.getInputs();

    // Map to track shared components (using digest as key)
    Map<RelNode, List<RelNode>> producerToConsumerMap = new HashMap<>();

    // Find shared components across all inputs
    findSharedComponents(combine, producerToConsumerMap);

    // Create spools for shared components and build a direct mapping
    // from each RelNode instance to its spool replacement
    Map<RelNode, LogicalTableSpool> nodeToSpoolMap = new HashMap<>();
    int spoolCounter = 0;

    for (Map.Entry<RelNode, List<RelNode>> entry : producerToConsumerMap.entrySet()) {
      RelNode producer = entry.getKey();
      List<RelNode> consumers = entry.getValue();

      if (consumers.size() > 1) {
        // This component is shared - create a spool for it

        // Create the spool table for temporary storage
        SpoolRelOptTable spoolTable = new SpoolRelOptTable(
            null,  // no schema needed for temporary tables
            producer.getRowType(),
            "spool_" + spoolCounter++
        );

        // Create the TableSpool that will produce/write to this table
        LogicalTableSpool spool =
            (LogicalTableSpool) RelFactories.DEFAULT_SPOOL_FACTORY.createTableSpool(
                producer,
                Spool.Type.LAZY,   // Read type
                Spool.Type.EAGER,  // Write type
                spoolTable
            );

        // Map the first consumer to the spool itself (producer)
        // and all others to the spool (they'll become consumers)
        nodeToSpoolMap.put(consumers.get(0), spool);
        for (int i = 1; i < consumers.size(); i++) {
          nodeToSpoolMap.put(consumers.get(i), spool);
        }
      }
    }

    // If no spools were created, nothing to do
    if (nodeToSpoolMap.isEmpty()) {
      return;
    }

    // Replace shared components in each input of the Combine using a shuttle
    List<RelNode> newInputs = new ArrayList<>();
    SpoolReplacementShuttle shuttle = new SpoolReplacementShuttle(nodeToSpoolMap);

    for (RelNode input : inputs) {
      // Apply the shuttle to replace shared components
      RelNode newInput = input.accept(shuttle);
      newInputs.add(newInput);
    }

    // Create the new Combine with the transformed inputs
    RelBuilder relBuilder = this.relBuilderFactory.create(
        combine.getCluster(),
        combine.getCluster().getPlanner().getContext().unwrap(RelOptSchema.class)
    );

    // Push all new inputs onto the builder
    for (RelNode newInput : newInputs) {
      relBuilder.push(newInput);
    }

    // Create the new Combine
    RelNode newCombine = relBuilder.combine(newInputs.size()).build();

    // Transform to the new plan
    call.transformTo(newCombine);
  }

  /**
   * Shuttle that replaces shared components with spools or table scans.
   */
  private static class SpoolReplacementShuttle extends RelShuttleImpl {
    private final Map<RelNode, LogicalTableSpool> nodeToSpoolMap;
    private final Set<LogicalTableSpool> usedSpools = new HashSet<>();

    SpoolReplacementShuttle(Map<RelNode, LogicalTableSpool> nodeToSpoolMap) {
      this.nodeToSpoolMap = nodeToSpoolMap;
    }

    @Override public RelNode visit(RelNode other) {
      // Check if this node should be replaced with a spool
      if (nodeToSpoolMap.containsKey(other)) {
        LogicalTableSpool spool = nodeToSpoolMap.get(other);

        // Check if this spool has been used already
        if (!usedSpools.contains(spool)) {
          // First use: return the spool itself (which writes to the table)
          usedSpools.add(spool);
          return spool;
        } else {
          // Subsequent uses: create a table scan that reads from the spool table
          RelOptTable spoolTable = spool.getTable();
          return LogicalTableScan.create(other.getCluster(), spoolTable, ImmutableList.of());
        }
      }

      // Otherwise, continue with default visiting behavior
      return super.visit(other.stripped());
    }
  }

  private void findSharedComponents(Combine combine, Map<RelNode, List<RelNode>> sharedComponentMap) {
    // Use digest to identify structurally identical nodes
    Map<String, List<RelNode>> digestToNodes = new HashMap<>();

    // Visit all nodes in all inputs
    for (RelNode input : combine.getInputs()) {
      input.accept(new RelHomogeneousShuttle() {
        @Override public RelNode visit(RelNode node) {
          // Strip to get the real node for comparison (handles HepRelVertex)
          RelNode stripped = node.stripped();
          String digest = stripped.getDigest();

          digestToNodes.computeIfAbsent(digest, k -> new ArrayList<>()).add(node);

          // Continue visiting children
          return super.visit(stripped);
        }
      });
    }

    // Now identify shared components (those with multiple occurrences)
    for (List<RelNode> nodes : digestToNodes.values()) {
      if (nodes.size() > 1) {
        RelNode first = nodes.get(0);
        sharedComponentMap.put(first, nodes);
      }
    }
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCombineSharedComponentsRule.Config.builder()
        .build()
        .withOperandFor(Combine.class);

    @Override default CombineSharedComponentsRule toRule() {
      return new CombineSharedComponentsRule(this);
    }

    default Config withOperandFor(Class<? extends Combine> combineClass) {
      return withOperandSupplier(b -> b.operand(combineClass).anyInputs())
          .as(Config.class);
    }
  }
}
