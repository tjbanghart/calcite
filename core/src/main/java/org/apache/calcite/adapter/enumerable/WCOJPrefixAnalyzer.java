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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Analyzes multiple {@link EnumerableWCOJ} operators to detect shared
 * variable prefixes using {@link JoinVariableFingerprint}s.
 *
 * <p>When multiple WCOJ operators within a Combine share a prefix of their
 * variable ordering (i.e., the first K variables are doing the same work
 * over the same inputs), this analyzer detects those groups so the prefix
 * can be executed once and shared.
 */
public final class WCOJPrefixAnalyzer {

  private WCOJPrefixAnalyzer() {
  }

  /**
   * Analyzes a list of WCOJ operators and returns groups that share
   * non-trivial variable prefixes.
   *
   * @param wcojOperators the WCOJ operators to analyze
   * @return list of prefix groups (only groups with depth >= 1 and size >= 2)
   */
  public static List<PrefixGroup> analyze(List<EnumerableWCOJ> wcojOperators) {
    if (wcojOperators.size() < 2) {
      return ImmutableList.of();
    }

    // Step 1: Compute fingerprint sequences for each WCOJ
    List<List<JoinVariableFingerprint>> fpSequences = new ArrayList<>();
    for (EnumerableWCOJ wcoj : wcojOperators) {
      List<JoinVariableFingerprint> seq = new ArrayList<>();
      for (EnumerableWCOJ.JoinVariable var : wcoj.getVariables()) {
        seq.add(JoinVariableFingerprint.create(var, wcoj.getInputs()));
      }
      fpSequences.add(seq);
    }

    // Step 2: Build a trie of fingerprint sequences and extract groups
    PrefixTrieNode root = new PrefixTrieNode();
    for (int i = 0; i < fpSequences.size(); i++) {
      root.insert(fpSequences.get(i), i);
    }

    // Step 3: Extract groups with shared prefixes
    List<PrefixGroup> groups = new ArrayList<>();
    root.extractGroups(groups, 0);
    return groups;
  }

  /**
   * A group of WCOJ operators that share a common variable prefix.
   */
  public static class PrefixGroup {
    /** Number of shared prefix levels. */
    public final int sharedPrefixDepth;

    /** Indices into the original WCOJ list for members of this group. */
    public final ImmutableList<Integer> memberIndices;

    PrefixGroup(int sharedPrefixDepth, List<Integer> memberIndices) {
      this.sharedPrefixDepth = sharedPrefixDepth;
      this.memberIndices = ImmutableList.copyOf(memberIndices);
    }

    @Override public String toString() {
      return "PrefixGroup(depth=" + sharedPrefixDepth
          + ", members=" + memberIndices + ")";
    }
  }

  /**
   * A trie node used during prefix analysis. Each node represents one level
   * of the fingerprint sequence. Children are keyed by the fingerprint at
   * the next level.
   */
  private static class PrefixTrieNode {
    final Map<JoinVariableFingerprint, PrefixTrieNode> children = new HashMap<>();
    final List<Integer> terminalQueryIndices = new ArrayList<>();

    void insert(List<JoinVariableFingerprint> sequence, int queryIndex) {
      PrefixTrieNode node = this;
      for (JoinVariableFingerprint fp : sequence) {
        node = node.children.computeIfAbsent(fp, k -> new PrefixTrieNode());
      }
      node.terminalQueryIndices.add(queryIndex);
    }

    /**
     * Collects all query indices reachable from this node (descendants + terminals).
     */
    List<Integer> collectAllQueries() {
      List<Integer> all = new ArrayList<>(terminalQueryIndices);
      for (PrefixTrieNode child : children.values()) {
        all.addAll(child.collectAllQueries());
      }
      return all;
    }

    /**
     * Extracts prefix groups from this trie node. A group is formed when
     * a node has multiple descendant queries (via different children),
     * meaning those queries shared the prefix up to this point but diverge here.
     */
    void extractGroups(List<PrefixGroup> groups, int depth) {
      if (depth > 0 && children.size() > 1) {
        // This node is a divergence point. All descendant queries share
        // the prefix up to 'depth' levels.
        List<Integer> allDescendants = collectAllQueries();
        if (allDescendants.size() >= 2) {
          groups.add(new PrefixGroup(depth, allDescendants));
        }
        // Don't recurse further — deeper shared prefixes within
        // subgroups would need separate grouping, but for now we
        // take the shallowest divergence point.
        return;
      }

      // If only one child, the prefix continues — recurse deeper
      for (PrefixTrieNode child : children.values()) {
        child.extractGroups(groups, depth + 1);
      }

      // Handle the case where some queries terminate at this level
      // while others continue: that's also a divergence
      if (depth > 0 && !terminalQueryIndices.isEmpty() && !children.isEmpty()) {
        List<Integer> allDescendants = collectAllQueries();
        if (allDescendants.size() >= 2) {
          groups.add(new PrefixGroup(depth, allDescendants));
        }
      }
    }
  }
}
