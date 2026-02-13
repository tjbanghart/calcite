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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Canonical fingerprint for a {@link EnumerableWCOJ.JoinVariable} that enables
 * comparison across different WCOJ operators.
 *
 * <p>Two JoinVariables from different WCOJ operators are "compatible" (doing
 * the same work) when they iterate the same key intersection over the same
 * physical inputs. The fingerprint captures this by using the structural
 * digest of each input RelNode and the field index, sorted canonically.
 *
 * <p>This is used by {@link WCOJPrefixAnalyzer} to detect shared variable
 * prefixes across WCOJ operators within a Combine.
 */
public final class JoinVariableFingerprint {

  /** Sorted list of (inputDigestString, fieldIndex) pairs. */
  private final ImmutableList<InputFieldRef> refs;

  private JoinVariableFingerprint(ImmutableList<InputFieldRef> refs) {
    this.refs = refs;
  }

  /**
   * Creates a fingerprint for a JoinVariable by resolving each occurrence
   * against the actual input RelNodes.
   */
  public static JoinVariableFingerprint create(
      EnumerableWCOJ.JoinVariable variable,
      List<RelNode> inputs) {
    List<InputFieldRef> refList = new ArrayList<>();
    for (Pair<Integer, Integer> occ : variable.occurrences) {
      String digest = inputs.get(occ.left).getRelDigest().toString();
      refList.add(new InputFieldRef(digest, occ.right));
    }
    refList.sort(Comparator.comparing((InputFieldRef r) -> r.digestString)
        .thenComparingInt(r -> r.fieldIndex));
    return new JoinVariableFingerprint(ImmutableList.copyOf(refList));
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JoinVariableFingerprint)) {
      return false;
    }
    return refs.equals(((JoinVariableFingerprint) o).refs);
  }

  @Override public int hashCode() {
    return refs.hashCode();
  }

  @Override public String toString() {
    return refs.toString();
  }

  /** A reference to a specific field in a specific input, identified by digest. */
  private static final class InputFieldRef {
    final String digestString;
    final int fieldIndex;

    InputFieldRef(String digestString, int fieldIndex) {
      this.digestString = digestString;
      this.fieldIndex = fieldIndex;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof InputFieldRef)) {
        return false;
      }
      InputFieldRef that = (InputFieldRef) o;
      return fieldIndex == that.fieldIndex
          && digestString.equals(that.digestString);
    }

    @Override public int hashCode() {
      return Objects.hash(digestString, fieldIndex);
    }

    @Override public String toString() {
      return "(" + digestString + ", " + fieldIndex + ")";
    }
  }
}
