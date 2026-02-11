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
package org.apache.calcite.linq4j.test;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.HashTrie;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HashTrie}.
 */
class HashTrieTest {

  @Test void testBuildSingleLevel() {
    // Build a single-level trie from (key, value) pairs
    List<Object[]> rows = Arrays.asList(
        new Object[]{"a", 1},
        new Object[]{"b", 2},
        new Object[]{"a", 3},
        new Object[]{"c", 4});

    List<Function1<Object[], @Nullable Object>> extractors =
        Collections.singletonList(row -> row[0]);

    HashTrie<Object[]> trie = HashTrie.build(Linq4j.asEnumerable(rows), extractors);

    assertFalse(trie.isEmpty());
    assertThat(trie.getDepth(), is(1));
  }

  @Test void testProbeSingleLevel() {
    List<Object[]> rows = Arrays.asList(
        new Object[]{"a", 1},
        new Object[]{"b", 2},
        new Object[]{"a", 3},
        new Object[]{"c", 4});

    List<Function1<Object[], @Nullable Object>> extractors =
        Collections.singletonList(row -> row[0]);

    HashTrie<Object[]> trie = HashTrie.build(Linq4j.asEnumerable(rows), extractors);

    // Probe for "a" - should return 2 rows
    List<Object[]> resultA = trie.probe(Collections.singletonList("a")).toList();
    assertThat(resultA.size(), is(2));
    Set<Object> values = new HashSet<>();
    for (Object[] row : resultA) {
      values.add(row[1]);
    }
    assertTrue(values.contains(1));
    assertTrue(values.contains(3));

    // Probe for "b" - should return 1 row
    List<Object[]> resultB = trie.probe(Collections.singletonList("b")).toList();
    assertThat(resultB.size(), is(1));
    assertThat(resultB.get(0)[1], is(2));

    // Probe for "d" - should return empty
    List<Object[]> resultD = trie.probe(Collections.singletonList("d")).toList();
    assertTrue(resultD.isEmpty());
  }

  @Test void testGetKeysAtLevel() {
    List<Object[]> rows = Arrays.asList(
        new Object[]{"a", 1},
        new Object[]{"b", 2},
        new Object[]{"a", 3},
        new Object[]{"c", 4});

    List<Function1<Object[], @Nullable Object>> extractors =
        Collections.singletonList(row -> row[0]);

    HashTrie<Object[]> trie = HashTrie.build(Linq4j.asEnumerable(rows), extractors);

    // Get keys at level 0
    Set<Object> keys = new HashSet<>();
    for (Object key : trie.getKeysAtLevel(0, Collections.emptyList())) {
      keys.add(key);
    }
    assertThat(keys.size(), is(3));
    assertTrue(keys.contains("a"));
    assertTrue(keys.contains("b"));
    assertTrue(keys.contains("c"));
  }

  @Test void testBuildTwoLevels() {
    // Build a two-level trie
    List<Object[]> rows = Arrays.asList(
        new Object[]{"a", 1, "x"},
        new Object[]{"a", 2, "y"},
        new Object[]{"b", 1, "z"});

    List<Function1<Object[], @Nullable Object>> extractors = Arrays.asList(
        row -> row[0],
        row -> row[1]);

    HashTrie<Object[]> trie = HashTrie.build(Linq4j.asEnumerable(rows), extractors);

    assertThat(trie.getDepth(), is(2));

    // Probe with just first key
    List<Object[]> resultA = trie.probe(Collections.singletonList("a")).toList();
    assertThat(resultA.size(), is(2));

    // Probe with both keys
    List<Object[]> resultA1 = trie.probe(Arrays.asList("a", 1)).toList();
    assertThat(resultA1.size(), is(1));
    assertThat(resultA1.get(0)[2], is("x"));
  }

  @Test void testNullKeysAreSkipped() {
    // Rows with null keys should be skipped during build
    List<Object[]> rows = Arrays.asList(
        new Object[]{"a", 1},
        new Object[]{null, 2},
        new Object[]{"c", 3});

    List<Function1<Object[], @Nullable Object>> extractors =
        Collections.singletonList(row -> row[0]);

    HashTrie<Object[]> trie = HashTrie.build(Linq4j.asEnumerable(rows), extractors);

    // Only 2 rows should be indexed (null key row is skipped)
    Set<Object> keys = new HashSet<>();
    for (Object key : trie.getKeysAtLevel(0, Collections.emptyList())) {
      keys.add(key);
    }
    assertThat(keys.size(), is(2));
    assertTrue(keys.contains("a"));
    assertTrue(keys.contains("c"));
    assertFalse(keys.contains(null));
  }

  @Test void testEmptyEnumerable() {
    List<Object[]> rows = Collections.emptyList();

    List<Function1<Object[], @Nullable Object>> extractors =
        Collections.singletonList(row -> row[0]);

    HashTrie<Object[]> trie = HashTrie.build(Linq4j.asEnumerable(rows), extractors);

    assertTrue(trie.isEmpty());
    assertTrue(trie.probe(Collections.singletonList("a")).toList().isEmpty());
  }

  @Test void testRequiresAtLeastOneExtractor() {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"a", 1});
    List<Function1<Object[], @Nullable Object>> extractors = Collections.emptyList();

    assertThrows(IllegalArgumentException.class,
        () -> HashTrie.build(Linq4j.asEnumerable(rows), extractors));
  }

  @Test void testProbeWithNullReturnsEmpty() {
    List<Object[]> rows = Arrays.asList(
        new Object[]{"a", 1},
        new Object[]{"b", 2});

    List<Function1<Object[], @Nullable Object>> extractors =
        Collections.singletonList(row -> row[0]);

    HashTrie<Object[]> trie = HashTrie.build(Linq4j.asEnumerable(rows), extractors);

    // Probe with null should return empty
    List<Object[]> result = trie.probe(Collections.singletonList(null)).toList();
    assertTrue(result.isEmpty());
  }
}
