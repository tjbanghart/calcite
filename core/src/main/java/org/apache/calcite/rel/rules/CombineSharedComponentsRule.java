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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.SpoolRelOptTable;
import org.apache.calcite.rel.RelCommonExpressionBasicSuggester;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
    RelNode combine = RelOptUtil.stripAll(call.rel(0));

    // Use the suggester to find shared components
    RelCommonExpressionBasicSuggester suggester = new RelCommonExpressionBasicSuggester();
    Collection<RelNode> sharedComponents = suggester.suggest(combine, null);

    // If no shared components found, nothing to do
    if (sharedComponents.isEmpty()) {
      return;
    }

    // Map to track which shared component gets which spool
    Map<RelNode, LogicalTableSpool> digestToSpool = new HashMap<>();
    // For each shared component, create a spool
    for (RelNode sharedComponent : sharedComponents) {
      // Create the spool table for temporary storage
      SpoolRelOptTable spoolTable = new SpoolRelOptTable(
          null,  // no schema needed for temporary tables
          sharedComponent.getRowType(),
          "spool_" + sharedComponent.getId()
      );

      // Create the TableSpool that will produce/write to this table
      LogicalTableSpool spool =
          (LogicalTableSpool) RelFactories.DEFAULT_SPOOL_FACTORY.createTableSpool(
              sharedComponent,
              Spool.Type.LAZY,  // Read type
              Spool.Type.LAZY,  // Write type
              spoolTable
          );

      digestToSpool.put(sharedComponent, spool);
    }

    combine = combine.accept(
        getReplacer(digestToSpool)
    );

    // Transform to the new plan
    call.transformTo(combine);
  }

  private static RelHomogeneousShuttle getReplacer(Map<RelNode, LogicalTableSpool> digestToSpool) {
    Set<RelNode> producers = new HashSet<>();

    return new RelHomogeneousShuttle() {
      @Override
      public RelNode visit(RelNode node) {
        // Check if this node matches any of our shared components
        if (digestToSpool.containsKey(node)) {
          LogicalTableSpool spool = digestToSpool.get(node);

          if (producers.contains(node)) {
            // Subsequent occurrence - replace with table scan (consumer)
            return LogicalTableScan.create(
                node.getCluster(),
                spool.getTable(),
                ImmutableList.of()
            );
          } else {
            // First occurrence - replace with the spool (producer)
            producers.add(node);
            return spool;
          }
        }
        // Continue visiting children
        return super.visit(node);
      }
    };
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
