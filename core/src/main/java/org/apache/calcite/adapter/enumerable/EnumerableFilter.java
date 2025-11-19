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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Filter} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableFilter
    extends Filter
    implements EnumerableRel {
  /** Creates an EnumerableFilter.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  public EnumerableFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() instanceof EnumerableConvention;
  }

  /** Creates an EnumerableFilter. */
  public static EnumerableFilter create(final RelNode input,
      RexNode condition) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.filter(mq, input))
            .replaceIf(RelDistributionTraitDef.INSTANCE,
                () -> RelMdDistribution.filter(mq, input));
    return new EnumerableFilter(cluster, traitSet, input, condition);
  }

  @Override public EnumerableFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new EnumerableFilter(getCluster(), traitSet, input, condition);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Visit child
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    final PhysType physType = result.physType;

    // child.where(row -> condition(row))
    Expression childExp = builder.append("child", result.block);

    // Generate predicate: row -> boolean
    final BlockBuilder predicateBuilder = new BlockBuilder();
    final org.apache.calcite.linq4j.tree.ParameterExpression row_ =
        Expressions.parameter(physType.getJavaRowType(), "row");

    final RexToLixTranslator.InputGetter inputGetter =
        new RexToLixTranslator.InputGetterImpl(row_, physType);

    final RexToLixTranslator translator =
        RexToLixTranslator.forAggregation(
            implementor.getTypeFactory(),
            predicateBuilder,
            inputGetter,
            implementor.getConformance());

    final Expression conditionExp = translator.translate(condition);

    // Ensure we return primitive boolean, not Boolean
    final Expression unboxedCondition =
        conditionExp.getType() == Boolean.class
            ? Expressions.unbox(conditionExp)
            : conditionExp;

    predicateBuilder.add(Expressions.return_(null, unboxedCondition));

    final Expression predicate =
        Expressions.lambda(
            org.apache.calcite.linq4j.function.Predicate1.class,
            predicateBuilder.toBlock(),
            row_);

    builder.add(
        Expressions.return_(null,
            Expressions.call(childExp, BuiltInMethod.WHERE.method, predicate)));

    return implementor.result(physType, builder.toBlock());
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      RelTraitSet required) {
    RelCollation collation = required.getCollation();
    if (collation == null || collation == RelCollations.EMPTY) {
      return null;
    }
    RelTraitSet traits = traitSet.replace(collation);
    return Pair.of(traits, ImmutableList.of(traits));
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    RelCollation collation = childTraits.getCollation();
    if (collation == null || collation == RelCollations.EMPTY) {
      return null;
    }
    RelTraitSet traits = traitSet.replace(collation);
    return Pair.of(traits, ImmutableList.of(traits));
  }
}
