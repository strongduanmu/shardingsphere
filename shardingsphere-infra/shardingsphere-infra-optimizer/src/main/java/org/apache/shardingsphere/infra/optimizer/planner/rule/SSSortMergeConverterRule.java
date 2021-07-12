/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.infra.optimizer.planner.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.shardingsphere.infra.optimizer.planner.ShardingSphereConvention;
import org.apache.shardingsphere.infra.optimizer.rel.physical.SSSortMergeJoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Join rule that converting LogicalJoin to SSSortMergeJoin.
 */
public final class SSSortMergeConverterRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE,
                    ShardingSphereConvention.INSTANCE, SSSortMergeConverterRule.class.getName())
            .withRuleFactory(SSSortMergeConverterRule::new);

    protected SSSortMergeConverterRule(final Config config) {
        super(config);
    }

    @Override
    public RelNode convert(final RelNode rel) {
        LogicalJoin logicalJoin = (LogicalJoin) rel;
        JoinInfo joinInfo = logicalJoin.analyzeCondition();
        if (joinInfo.pairs().isEmpty()) {
            return null;
        }

        List<RelCollation> newCollations = new ArrayList<>();
        int rightOffset = logicalJoin.getLeft().getRowType().getFieldCount();
        RelNode newLeft = convertRelNode(logicalJoin.getLeft(), joinInfo.leftKeys, newCollations, 0);
        RelNode newRight = convertRelNode(logicalJoin.getRight(), joinInfo.rightKeys, newCollations, rightOffset);

        RexNode condition;
        RexBuilder rexBuilder = logicalJoin.getCluster().getRexBuilder();
        RexNode equi = joinInfo.getEquiCondition(newLeft, newRight, rexBuilder);
        if (joinInfo.isEqui()) {
            condition = equi;
        } else {
            RexNode nonEqui = RexUtil.composeConjunction(rexBuilder, joinInfo.nonEquiConditions);
            condition = RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui));
        }

        return SSSortMergeJoin.create(newLeft, newRight, condition, logicalJoin.getVariablesSet(), logicalJoin.getJoinType(), newCollations);
    }

    private RelNode convertRelNode(final RelNode node, final ImmutableIntList joinKeys, final List<RelCollation> collations, final int offset) {
        RelTraitSet traits = node.getTraitSet();
        List<RelFieldCollation> fieldCollations = joinKeys.stream().map(key -> newRelFieldCollation(key)).collect(Collectors.toList());
        final RelCollation collation = RelCollations.of(fieldCollations);
        collations.add(RelCollations.shift(collation, offset));
        traits = traits.replace(collation);
        return convert(node, traits);
    }

    private RelFieldCollation newRelFieldCollation(final int key) {
        return new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.LAST);
    }
}
