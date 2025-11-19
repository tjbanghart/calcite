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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Project} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableProject extends Project implements EnumerableRel {
  /**
   * Creates an EnumerableProject.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet Traits of this relational expression
   * @param input    Input relational expression
   * @param projects List of expressions for the input columns
   * @param rowType  Output row type
   */
  public EnumerableProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    assert getConvention() instanceof EnumerableConvention;
  }

  @Deprecated // to be removed before 2.0
  public EnumerableProject(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType,
      int flags) {
    this(cluster, traitSet, input, projects, rowType);
    Util.discard(flags);
  }

  /** Creates an EnumerableProject, specifying row type rather than field
   * names. */
  public static EnumerableProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.project(mq, input, projects));
    return new EnumerableProject(cluster, traitSet, input, projects, rowType);
  }

  @Override public EnumerableProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new EnumerableProject(getCluster(), traitSet, input,
        projects, rowType);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Visit child
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(result.format));

    // child.select(row -> new Row(projections...))
    Expression childExp = builder.append("child", result.block);

    // Generate selector: row -> outputRow
    final BlockBuilder selectorBuilder = new BlockBuilder();
    final org.apache.calcite.linq4j.tree.ParameterExpression row_ =
        Expressions.parameter(result.physType.getJavaRowType(), "row");

    final RexToLixTranslator.InputGetter inputGetter =
        new RexToLixTranslator.InputGetterImpl(row_, result.physType);

    final RexToLixTranslator translator =
        RexToLixTranslator.forAggregation(
            implementor.getTypeFactory(),
            selectorBuilder,
            inputGetter,
            implementor.getConformance());

    final List<Expression> translatedExps = new java.util.ArrayList<>();
    for (RexNode exp : exps) {
      translatedExps.add(translator.translate(exp));
    }

    selectorBuilder.add(
        Expressions.return_(null, physType.record(translatedExps)));

    final Expression selector =
        Expressions.lambda(
            org.apache.calcite.linq4j.function.Function1.class,
            selectorBuilder.toBlock(),
            row_);

    builder.add(
        Expressions.return_(null,
            Expressions.call(childExp, BuiltInMethod.SELECT.method, selector)));

    return implementor.result(physType, builder.toBlock());
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      RelTraitSet required) {
    return EnumerableTraitsUtils.passThroughTraitsForProject(required, exps,
        input.getRowType(), input.getCluster().getTypeFactory(), traitSet);
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    return EnumerableTraitsUtils.deriveTraitsForProject(childTraits, childId, exps,
        input.getRowType(), input.getCluster().getTypeFactory(), traitSet);
  }
}
