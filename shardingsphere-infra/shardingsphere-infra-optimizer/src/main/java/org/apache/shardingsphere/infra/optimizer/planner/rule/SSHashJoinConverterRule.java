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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.shardingsphere.infra.optimizer.planner.ShardingSphereConvention;
import org.apache.shardingsphere.infra.optimizer.rel.physical.SSHashJoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Join rule that converting <code>LogicalJoin</code> to <code>SSHashJoin</code>.
 */
public final class SSHashJoinConverterRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE,
                    ShardingSphereConvention.INSTANCE, SSHashJoinConverterRule.class.getName())
            .withRuleFactory(SSHashJoinConverterRule::new);
    
    protected SSHashJoinConverterRule(final Config config) {
        super(config);
    }

    @Override
    public RelNode convert(final RelNode rel) {
        LogicalJoin logicalJoin = (LogicalJoin) rel;
        JoinInfo info = logicalJoin.analyzeCondition();
        boolean hasEquiKeys = !info.leftKeys.isEmpty() && !info.rightKeys.isEmpty();
        if (!hasEquiKeys) {
            // hash join can only be used if equal keys exist.
            return null;
        }
        
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : logicalJoin.getInputs()) {
            RelNode newInput = input;
            if (!(input.getConvention() instanceof ShardingSphereConvention)) {
                newInput = convert(input, input.getTraitSet().replace(ShardingSphereConvention.INSTANCE));
            }
            newInputs.add(newInput);
        }
        
        RelNode left = newInputs.get(0);
        RelNode right = newInputs.get(1);
        RexBuilder rexBuilder = logicalJoin.getCluster().getRexBuilder();
        RexNode equi = info.getEquiCondition(left, right, rexBuilder);
        RexNode condition;
        if (info.isEqui()) {
            condition = equi;
        } else {
            RexNode nonEqui = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions);
            condition = RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui));
        }
        return SSHashJoin.create(left, right, condition, logicalJoin.getVariablesSet(), logicalJoin.getJoinType());
    }
}
