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
package org.apache.calcite.linq4j;

import org.apache.calcite.linq4j.function.Function1;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Multi-level hash structure for Worst-Case Optimal Join (WCOJ) probing.
 *
 * <p>Each level corresponds to a join attribute. Leaf nodes store row references.
 * Supports efficient multi-key lookup and iteration for the WCOJ algorithm.
 *
 * <p>The trie is built from an enumerable using ordered key extractors.
 * During join execution, the trie is probed with partial key prefixes to
 * find matching rows efficiently.
 *
 * @param <TRow> the row type stored in the trie
 */
public class HashTrie<TRow> {
  /** The root node of the trie. */
  private final HashTrieNode<TRow> root;

  /** The number of key levels in this trie. */
  private final int depth;

  /**
   * Creates a HashTrie with the given root and depth.
   */
  private HashTrie(HashTrieNode<TRow> root, int depth) {
    this.root = root;
    this.depth = depth;
  }

  /**
   * Builds a trie from an enumerable with ordered key extractors.
   *
   * <p>Each key extractor corresponds to one level in the trie. Rows are
   * indexed by extracting keys in order and navigating/creating the trie structure.
   *
   * @param source the source enumerable
   * @param keyExtractors ordered list of functions to extract keys from rows
   * @param <TRow> the row type
   * @return a new HashTrie indexing the source data
   */
  public static <TRow> HashTrie<TRow> build(
      Enumerable<TRow> source,
      List<Function1<TRow, @Nullable Object>> keyExtractors) {
    final int depth = keyExtractors.size();
    if (depth == 0) {
      throw new IllegalArgumentException("At least one key extractor is required");
    }

    final HashTrieNode<TRow> root = new HashTrieNode<>();

    try (Enumerator<TRow> enumerator = source.enumerator()) {
      while (enumerator.moveNext()) {
        TRow row = enumerator.current();
        insertRow(root, row, keyExtractors, 0);
      }
    }

    return new HashTrie<>(root, depth);
  }

  /**
   * Recursively inserts a row into the trie.
   */
  private static <TRow> void insertRow(
      HashTrieNode<TRow> node,
      TRow row,
      List<Function1<TRow, @Nullable Object>> keyExtractors,
      int level) {
    if (level == keyExtractors.size() - 1) {
      // Last level: store in leaf
      Object key = keyExtractors.get(level).apply(row);
      if (key == null) {
        // Skip rows with null keys
        return;
      }
      HashTrieNode<TRow> child = node.getChild(key);
      if (child == null) {
        child = new HashTrieNode<>(Collections.singletonList(row));
        node.setChild(key, child);
      } else if (child.isLeaf()) {
        child.addRow(row);
      } else {
        // Should not happen with consistent depth
        throw new IllegalStateException("Expected leaf node at depth " + level);
      }
    } else {
      // Interior level: navigate to child
      Object key = keyExtractors.get(level).apply(row);
      if (key == null) {
        // Skip rows with null keys
        return;
      }
      HashTrieNode<TRow> child = node.getOrCreateChild(key);
      insertRow(child, row, keyExtractors, level + 1);
    }
  }

  /**
   * Probes the trie with a partial key prefix and returns matching rows.
   *
   * <p>The key values list should have one entry per level being probed.
   * If the list is shorter than the trie depth, returns all rows in the subtrie.
   *
   * @param keyValues the key values to probe with (one per level)
   * @return an enumerable of matching rows
   */
  public Enumerable<TRow> probe(List<@Nullable Object> keyValues) {
    HashTrieNode<TRow> node = root;

    // Navigate through the trie using the provided keys
    for (Object keyValue : keyValues) {
      if (keyValue == null) {
        return Linq4j.emptyEnumerable();
      }
      node = node.getChild(keyValue);
      if (node == null) {
        return Linq4j.emptyEnumerable();
      }
    }

    // Collect all rows in the subtrie rooted at this node
    List<TRow> results = new ArrayList<>();
    collectRows(node, results);
    return Linq4j.asEnumerable(results);
  }

  /**
   * Recursively collects all rows from a node and its descendants.
   */
  private void collectRows(HashTrieNode<TRow> node, List<TRow> results) {
    if (node.isLeaf()) {
      results.addAll(node.getRows());
    } else {
      for (Object key : node.getKeys()) {
        HashTrieNode<TRow> child = node.getChild(key);
        if (child != null) {
          collectRows(child, results);
        }
      }
    }
  }

  /**
   * Gets distinct values at a specified level given a key prefix.
   *
   * <p>This is used for leapfrog iteration to enumerate all possible
   * values at a given join variable position.
   *
   * @param level the trie level (0-indexed)
   * @param prefix key values for levels 0 to level-1
   * @return enumerable of distinct key values at the specified level
   */
  public Enumerable<Object> getKeysAtLevel(int level, List<@Nullable Object> prefix) {
    if (level < 0 || level >= depth) {
      throw new IllegalArgumentException("Level must be between 0 and " + (depth - 1));
    }
    if (prefix.size() != level) {
      throw new IllegalArgumentException(
          "Prefix size must equal level. Expected " + level + ", got " + prefix.size());
    }

    HashTrieNode<TRow> node = root;

    // Navigate to the node at the specified level
    for (Object keyValue : prefix) {
      if (keyValue == null) {
        return Linq4j.emptyEnumerable();
      }
      node = node.getChild(keyValue);
      if (node == null) {
        return Linq4j.emptyEnumerable();
      }
    }

    // Return keys at this level
    if (node.isLeaf()) {
      return Linq4j.emptyEnumerable();
    }
    Set<Object> keys = node.getKeys();
    return Linq4j.asEnumerable(keys);
  }

  /**
   * Returns the root node of this trie.
   * Package-private for testing.
   */
  HashTrieNode<TRow> getRoot() {
    return root;
  }

  /**
   * Returns the depth (number of levels) of this trie.
   */
  public int getDepth() {
    return depth;
  }

  /**
   * Returns whether the trie is empty.
   */
  public boolean isEmpty() {
    return !root.hasChildren();
  }
}
