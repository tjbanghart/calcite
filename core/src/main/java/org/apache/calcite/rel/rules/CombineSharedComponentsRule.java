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
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

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

    // Replace shared components in each input of the Combine
    List<RelNode> newInputs = new ArrayList<>();
    Set<LogicalTableSpool> usedSpools = new HashSet<>();

    for (RelNode input : inputs) {
      // Replace shared components with spools or table scans
      RelNode newInput = replaceSharedComponents(input, nodeToSpoolMap, usedSpools);
      newInputs.add(newInput);
    }

    // Create the new Combine with the transformed inputs
    RelBuilder relBuilder = call.builder();

    for (RelNode input : newInputs) {
      relBuilder.push(input);
    }

    relBuilder.combine();

    for (LogicalTableSpool spool : usedSpools) {
      relBuilder.push(spool);
    }

    // Transform to the new plan
    call.transformTo(relBuilder.build());
  }

  private RelNode replaceSharedComponents(RelNode node,
      Map<RelNode, LogicalTableSpool> nodeToSpoolMap,
      Set<LogicalTableSpool> usedSpools) {
    // If this node is mapped to a spool, replace it
    if (nodeToSpoolMap.containsKey(node)) {
      LogicalTableSpool spool = nodeToSpoolMap.get(node);
      usedSpools.add(spool);
      RelOptTable spoolTable = spool.getTable();
      return LogicalTableScan.create(node.getCluster(), spoolTable, ImmutableList.of());
    }

    // Otherwise, recursively replace in children
    List<RelNode> newInputs = new ArrayList<>();
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode newInput = replaceSharedComponents(input, nodeToSpoolMap, usedSpools);
      newInputs.add(newInput);
      if (newInput != input) {
        changed = true;
      }
    }

    if (changed) {
      return node.copy(node.getTraitSet(), newInputs);
    }

    return node;
  }

  private void findSharedComponents(Combine combine, Map<RelNode, List<RelNode>> sharedComponentMap) {
    // Use digest to identify structurally identical nodes
    Map<String, RelNode> digestToNode = new HashMap<>();

    // Visit all nodes in all inputs
    for (RelNode input : combine.getInputs()) {
      input.accept(new RelHomogeneousShuttle() {
        @Override public RelNode visit(RelNode other) {
          String digest = other.getDigest();

          // Check if we've seen this digest before
          if (digestToNode.containsKey(digest)) {
            // This is a shared component - add to the list of consumers
            RelNode firstOccurrence = digestToNode.get(digest);
            sharedComponentMap.computeIfAbsent(firstOccurrence, k -> new ArrayList<>()).add(other);
          } else {
            // First time seeing this digest
            digestToNode.put(digest, other);
            // Initialize with the first occurrence
            sharedComponentMap.computeIfAbsent(other, k -> new ArrayList<>()).add(other);
          }

          return super.visit(other);
        }
      });
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
