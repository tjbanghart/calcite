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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rule that optimizes a {@link EnumerableCombine} containing multiple
 * {@link EnumerableWCOJ} children by detecting shared variable prefixes
 * and rewriting them to use shared prefix computation.
 *
 * <p>When two or more WCOJ operators share the first K variables (i.e.,
 * the same join conditions over the same inputs in the same order), this
 * rule rewrites them to compute the shared prefix once and inject the
 * bindings into each suffix WCOJ.
 *
 * <p>The rewritten WCOJ operators are instances of
 * {@link EnumerableWCOJWithPrefix}, which stores the shared prefix depth
 * and a reference to the group identifier (used during code generation
 * to ensure the prefix is computed once per group).
 */
@Value.Enclosing
public class EnumerableCombineWCOJPrefixRule
    extends RelRule<EnumerableCombineWCOJPrefixRule.Config> {

  protected EnumerableCombineWCOJPrefixRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    EnumerableCombine combine = call.rel(0);

    // Identify WCOJ children
    List<Integer> wcojIndices = new ArrayList<>();
    List<EnumerableWCOJ> wcojChildren = new ArrayList<>();
    for (int i = 0; i < combine.getInputs().size(); i++) {
      RelNode child = combine.getInputs().get(i);
      if (child instanceof EnumerableWCOJ) {
        wcojIndices.add(i);
        wcojChildren.add((EnumerableWCOJ) child);
      }
    }

    if (wcojChildren.size() < 2) {
      return; // Need at least 2 WCOJs to share a prefix
    }

    // Analyze for shared prefixes
    List<WCOJPrefixAnalyzer.PrefixGroup> groups =
        WCOJPrefixAnalyzer.analyze(wcojChildren);

    if (groups.isEmpty()) {
      return; // No shared prefixes found
    }

    // Build the new input list for the Combine.
    // Replace grouped WCOJ children with EnumerableWCOJWithPrefix nodes.
    List<RelNode> newInputs = new ArrayList<>(combine.getInputs());
    Set<Integer> rewrittenWcojIndices = new HashSet<>();

    for (int groupId = 0; groupId < groups.size(); groupId++) {
      WCOJPrefixAnalyzer.PrefixGroup group = groups.get(groupId);

      for (int memberIdx : group.memberIndices) {
        int combineChildIdx = wcojIndices.get(memberIdx);
        EnumerableWCOJ originalWcoj = wcojChildren.get(memberIdx);

        // Create a prefix-aware wrapper
        EnumerableWCOJWithPrefix prefixWcoj =
            EnumerableWCOJWithPrefix.create(originalWcoj, group.sharedPrefixDepth,
                groupId);

        newInputs.set(combineChildIdx, prefixWcoj);
        rewrittenWcojIndices.add(combineChildIdx);
      }
    }

    // Only transform if we actually rewrote something
    if (rewrittenWcojIndices.isEmpty()) {
      return;
    }

    RelNode newCombine = combine.copy(combine.getTraitSet(), newInputs);
    call.transformTo(newCombine);
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableEnumerableCombineWCOJPrefixRule.Config.builder()
        .build()
        .withOperandFor();

    @Override default EnumerableCombineWCOJPrefixRule toRule() {
      return new EnumerableCombineWCOJPrefixRule(this);
    }

    default Config withOperandFor() {
      return withOperandSupplier(b -> b.operand(EnumerableCombine.class)
          .predicate(combine -> {
            // Only fire if there are at least 2 WCOJ children
            int wcojCount = 0;
            for (RelNode input : combine.getInputs()) {
              if (input instanceof EnumerableWCOJ) {
                wcojCount++;
              }
            }
            return wcojCount >= 2;
          })
          .anyInputs())
          .as(Config.class);
    }
  }
}
