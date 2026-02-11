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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Internal node representation for {@link HashTrie}.
 *
 * <p>Each node is either an interior node with children (mapping keys to child nodes)
 * or a leaf node containing a list of rows.
 *
 * @param <TRow> the row type stored in the trie
 */
class HashTrieNode<TRow> {
  /** Children of this node, mapping key values to child nodes. */
  private final @Nullable Map<Object, HashTrieNode<TRow>> children;

  /** Rows stored at this leaf node. */
  private final @Nullable List<TRow> rows;

  /** Whether this is a leaf node. */
  private final boolean isLeaf;

  /**
   * Creates an interior (non-leaf) node.
   */
  HashTrieNode() {
    this.children = new HashMap<>();
    this.rows = null;
    this.isLeaf = false;
  }

  /**
   * Creates a leaf node.
   *
   * @param rows initial list of rows (will be copied)
   */
  HashTrieNode(List<TRow> rows) {
    this.children = null;
    this.rows = new ArrayList<>(rows);
    this.isLeaf = true;
  }

  /**
   * Returns whether this is a leaf node.
   */
  boolean isLeaf() {
    return isLeaf;
  }

  /**
   * Returns the child node for the given key, or null if not present.
   *
   * @param key the key to look up
   * @return the child node, or null
   * @throws IllegalStateException if this is a leaf node
   */
  @Nullable HashTrieNode<TRow> getChild(Object key) {
    if (isLeaf) {
      throw new IllegalStateException("Cannot get child of a leaf node");
    }
    assert children != null;
    return children.get(key);
  }

  /**
   * Adds or retrieves a child node for the given key.
   *
   * @param key the key
   * @return the existing or newly created child node
   * @throws IllegalStateException if this is a leaf node
   */
  HashTrieNode<TRow> getOrCreateChild(Object key) {
    if (isLeaf) {
      throw new IllegalStateException("Cannot create child in a leaf node");
    }
    assert children != null;
    return children.computeIfAbsent(key, k -> new HashTrieNode<>());
  }

  /**
   * Sets a child node for the given key.
   *
   * @param key the key
   * @param child the child node
   * @throws IllegalStateException if this is a leaf node
   */
  void setChild(Object key, HashTrieNode<TRow> child) {
    if (isLeaf) {
      throw new IllegalStateException("Cannot set child in a leaf node");
    }
    assert children != null;
    children.put(key, child);
  }

  /**
   * Returns the set of keys at this node.
   *
   * @return the set of child keys
   * @throws IllegalStateException if this is a leaf node
   */
  Set<Object> getKeys() {
    if (isLeaf) {
      throw new IllegalStateException("Cannot get keys of a leaf node");
    }
    assert children != null;
    return children.keySet();
  }

  /**
   * Returns the rows stored at this leaf node.
   *
   * @return the list of rows
   * @throws IllegalStateException if this is not a leaf node
   */
  List<TRow> getRows() {
    if (!isLeaf) {
      throw new IllegalStateException("Cannot get rows of a non-leaf node");
    }
    assert rows != null;
    return rows;
  }

  /**
   * Adds a row to this leaf node.
   *
   * @param row the row to add
   * @throws IllegalStateException if this is not a leaf node
   */
  void addRow(TRow row) {
    if (!isLeaf) {
      throw new IllegalStateException("Cannot add row to a non-leaf node");
    }
    assert rows != null;
    rows.add(row);
  }

  /**
   * Returns whether this node has any children.
   *
   * @return true if this interior node has children
   * @throws IllegalStateException if this is a leaf node
   */
  boolean hasChildren() {
    if (isLeaf) {
      throw new IllegalStateException("Leaf node does not have children");
    }
    assert children != null;
    return !children.isEmpty();
  }
}
