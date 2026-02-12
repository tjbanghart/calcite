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

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cache for {@link HashTrie} instances, allowing reuse of tries across
 * multiple WCOJ operators that share the same input data.
 *
 * <p>When multiple WCOJ joins within a {@code Combine} operate on the same
 * input relation indexed on the same field, building the trie once and reusing
 * it avoids redundant work. The cache uses identity comparison on the input
 * enumerable, so sharing only occurs when inputs are the same object
 * (e.g., a shared spool).
 *
 * <p>This cache is intended to be created per-execution (e.g., by an
 * {@code EnumerableCombine}) and shared across its child WCOJ operators.
 */
public class TrieCache {
  /**
   * Two-level cache: first by input identity, then by field index.
   * Using IdentityHashMap ensures we only reuse tries when the underlying
   * data source is the exact same object reference.
   */
  private final IdentityHashMap<Enumerable<Object[]>, Map<Integer, HashTrie<Object[]>>>
      cache = new IdentityHashMap<>();

  /**
   * Gets or builds a single-level trie for the given input and field.
   *
   * @param input the input enumerable
   * @param fieldIdx the field index to use as the trie key
   * @return the cached or newly built trie
   */
  public HashTrie<Object[]> getOrBuild(
      Enumerable<Object[]> input, int fieldIdx) {
    return cache
        .computeIfAbsent(input, k -> new HashMap<>())
        .computeIfAbsent(fieldIdx, k -> {
          List<Function1<Object[], @Nullable Object>> extractors =
              Collections.singletonList(row -> row[fieldIdx]);
          return HashTrie.build(input, extractors);
        });
  }
}
