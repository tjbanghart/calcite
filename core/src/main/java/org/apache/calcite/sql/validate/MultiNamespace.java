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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Namespace for the MULTI operator.
 *
 * <p>MULTI combines multiple independent queries and returns their results
 * as a nested collection.
 */
public class MultiNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlCall multi;
  private final SqlValidatorScope scope;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MultiNamespace.
   *
   * @param validator     Validator
   * @param multi         MULTI parse tree node
   * @param scope         Scope
   * @param enclosingNode Enclosing node
   */
  MultiNamespace(
      SqlValidatorImpl validator,
      SqlCall multi,
      SqlValidatorScope scope,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.multi = multi;
    this.scope = scope;
  }

  //~ Methods ----------------------------------------------------------------

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    // The operands have already been registered by registerQuery.
    // Now we just need to get their validated types.
    final List<RelDataType> types = new ArrayList<>();
    for (SqlNode operand : multi.getOperandList()) {
      final RelDataType type = validator.getValidatedNodeType(operand);
      types.add(type);
    }

    // Build a row type that contains each query result as a field
    // Each query produces multiple rows, so we wrap the row type in an ARRAY
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < types.size(); i++) {
      // Wrap each query's row type in an ARRAY type since queries produce multiple rows
      final RelDataType arrayType = typeFactory.createArrayType(types.get(i), -1);
      builder.add("EXPR$" + i, arrayType);
    }
    return builder.build();
  }

  @Override public @Nullable SqlNode getNode() {
    return multi;
  }

  /**
   * Returns the scope.
   *
   * @return scope
   */
  public SqlValidatorScope getScope() {
    return scope;
  }

  @Override public boolean supportsModality(SqlModality modality) {
    return modality == SqlModality.RELATION;
  }
}
