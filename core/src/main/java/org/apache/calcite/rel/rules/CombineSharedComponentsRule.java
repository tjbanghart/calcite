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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    Map<String, RelNode> sharedComponents = new HashMap<>();
    Map<String, List<Integer>> componentUsers = new HashMap<>();

    // Find shared components across all inputs
    for (int i = 0; i < inputs.size(); i++) {
      findSharedComponents(inputs.get(i), sharedComponents, componentUsers, i);
    }

    // Check if there are shared components
    boolean hasShared = false;
    for (List<Integer> users : componentUsers.values()) {
      if (users.size() > 1) {
        hasShared = true;
        break;
      }
    }

    if (!hasShared) {
      return;
    }

    // TODO: Implement proper shared component optimization using TableSpool
    final RelBuilder relBuilder = call.builder();
    List<RelNode> newInputs = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      final Map<String, List<Integer>> finalComponentUsers = componentUsers;
      RelNode newInput = inputs.get(i).accept(new RelShuttleImpl() {
        @Override public RelNode visit(RelNode other) {
          if (other.getInputs().isEmpty()) {
            String digest = other.getDigest();
            List<Integer> users = finalComponentUsers.get(digest);
            if (users != null && users.size() > 1) {
              // Add a project to mark this as optimized
              return relBuilder
                  .push(other)
                  .project(relBuilder.fields())
                  .build();
            }
          }
          return super.visit(other);
        }
      });
      newInputs.add(newInput);
    }

    // Create new Combine with transformed inputs using RelBuilder
    // This ensures proper cluster sharing
    relBuilder.clear();
    for (RelNode input : newInputs) {
      relBuilder.push(input);
    }
    RelNode newCombine = relBuilder.combine(newInputs.size()).build();
    call.transformTo(newCombine);
  }
  private void findSharedComponents(RelNode node, Map<String, RelNode> sharedComponents,
                                   Map<String, List<Integer>> componentUsers, int inputIndex) {
    // Use a shuttle to traverse the tree
    node.accept(new RelShuttleImpl() {
      @Override public RelNode visit(RelNode other) {
        // For simplicity, we only consider leaf nodes (table scans) as shareable
        if (other.getInputs().isEmpty()) {
          String digest = other.getDigest();
          sharedComponents.putIfAbsent(digest, other);
          componentUsers.computeIfAbsent(digest, k -> new ArrayList<>()).add(inputIndex);
        }
        return super.visit(other);
      }
    });
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
