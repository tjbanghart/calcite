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
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.core.TableSpool;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rule that combines multiple TableSpool operators (CTEs) into a single
 * Combine operator along with the main query.
 *
 * <p>This rule detects patterns where multiple TableSpool operators exist
 * (indicating multiple CTEs) and wraps them with a Combine operator to
 * enable independent optimization of each CTE.
 *
 * <p>Similar to how SubQueryRemoveRule transforms subqueries, this rule
 * performs structural transformation to handle multiple CTEs.
 */
@Value.Enclosing
public class CombineMultipleSpoolRule extends RelRule<CombineMultipleSpoolRule.Config> {

  /** Creates a CombineMultipleSpoolRule. */
  protected CombineMultipleSpoolRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    RelNode rel = call.rel(0);

    // Skip if already wrapped in Combine
    if (rel instanceof Combine) {
      return false;
    }

    // Check if there are multiple CTEs (TableSpools)
    SpoolDetector detector = new SpoolDetector();
    detector.visit(rel);

    // Only match if we have multiple spools
    return detector.getSpools().size() > 1;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelNode root = call.rel(0);

    // Detect all TableSpools
    SpoolDetector detector = new SpoolDetector();
    detector.visit(root);

    List<TableSpool> spools = detector.getSpools();
    if (spools.size() <= 1) {
      return; // No need for Combine with single or no CTEs
    }

    // Build the Combine structure
    List<RelNode> combineInputs = new ArrayList<>();

    // Add each CTE definition (input of the spool)
    for (TableSpool spool : spools) {
      // Add the CTE definition (the input of the spool)
      combineInputs.add(spool.getInput());
    }

    // Add the main query (the root node with spools replaced by references)
    // For simplicity, we add the root as-is
    // In a full implementation, we would replace spool reads with references
    combineInputs.add(root);

    // Create the Combine operator
    Combine combine = Combine.create(
        root.getCluster(),
        root.getTraitSet(),
        combineInputs);

    call.transformTo(combine);
  }

  /**
   * Visitor that detects TableSpool operators in the tree.
   */
  private static class SpoolDetector {
    private final Set<TableSpool> spools = new HashSet<>();
    private final Set<RelNode> visited = new HashSet<>();

    public List<TableSpool> getSpools() {
      return new ArrayList<>(spools);
    }

    public void visit(RelNode node) {
      if (visited.contains(node)) {
        return;
      }
      visited.add(node);

      if (node instanceof TableSpool) {
        TableSpool spool = (TableSpool) node;
        // Only consider LAZY spools (CTEs that haven't been materialized)
        if (spool.readType == TableSpool.Type.LAZY) {
          spools.add(spool);
        }
      }

      // Visit children
      for (RelNode input : node.getInputs()) {
        visit(input);
      }
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCombineMultipleSpoolRule.Config.builder()
        .build()
        .withOperandSupplier(b0 ->
            b0.operand(RelNode.class)
                .predicate(rel -> !(rel instanceof Combine))
                .anyInputs())
        .withDescription("CombineMultipleSpoolRule");

    @Override default CombineMultipleSpoolRule toRule() {
      return new CombineMultipleSpoolRule(this);
    }
  }
}
