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

package org.apache.shardingsphere.traffic.rule;

import com.google.common.base.Preconditions;
import org.apache.shardingsphere.infra.binder.LogicSQL;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.type.SegmentAvailable;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmFactory;
import org.apache.shardingsphere.infra.rule.identifier.scope.GlobalRule;
import org.apache.shardingsphere.spi.ShardingSphereServiceLoader;
import org.apache.shardingsphere.sql.parser.sql.common.segment.SQLSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.CommentSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.AbstractSQLStatement;
import org.apache.shardingsphere.traffic.api.config.TrafficRuleConfiguration;
import org.apache.shardingsphere.traffic.api.traffic.hint.HintTrafficAlgorithm;
import org.apache.shardingsphere.traffic.api.traffic.hint.HintTrafficValue;
import org.apache.shardingsphere.traffic.api.traffic.segment.SegmentTrafficAlgorithm;
import org.apache.shardingsphere.traffic.api.traffic.segment.SegmentTrafficValue;
import org.apache.shardingsphere.traffic.spi.TrafficAlgorithm;
import org.apache.shardingsphere.traffic.spi.TrafficLoadBalanceAlgorithm;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Traffic rule.
 */
public final class TrafficRule implements GlobalRule {
    
    static {
        ShardingSphereServiceLoader.register(TrafficAlgorithm.class);
        ShardingSphereServiceLoader.register(TrafficLoadBalanceAlgorithm.class);
    }
    
    private final Collection<TrafficStrategyRule> trafficStrategyRules = new LinkedList<>();
    
    private final Map<String, TrafficAlgorithm> trafficAlgorithms = new LinkedHashMap<>();
    
    private final Map<String, TrafficLoadBalanceAlgorithm> loadBalancers = new LinkedHashMap<>();
    
    public TrafficRule(final TrafficRuleConfiguration config) {
        config.getTrafficStrategies().forEach(each -> trafficStrategyRules.add(new TrafficStrategyRule(each.getName(), each.getLabels(), each.getAlgorithmName(), each.getLoadBalancerName())));
        config.getTrafficAlgorithms().forEach((key, value) -> trafficAlgorithms.put(key, ShardingSphereAlgorithmFactory.createAlgorithm(value, TrafficAlgorithm.class)));
        config.getLoadBalancers().forEach((key, value) -> loadBalancers.put(key, ShardingSphereAlgorithmFactory.createAlgorithm(value, TrafficLoadBalanceAlgorithm.class)));
    }
    
    @Override
    public String getType() {
        return TrafficRule.class.getSimpleName();
    }
    
    /**
     * Find matched strategy rule.
     * 
     * @param logicSQL logic SQL
     * @return matched strategy rule
     */
    public Optional<TrafficStrategyRule> findMatchedStrategyRule(final LogicSQL logicSQL) {
        for (TrafficStrategyRule each : trafficStrategyRules) {
            TrafficAlgorithm trafficAlgorithm = trafficAlgorithms.get(each.getAlgorithmName());
            Preconditions.checkState(null != trafficAlgorithm, "Traffic strategy rule configuration must match traffic algorithm.");
            if (match(trafficAlgorithm, logicSQL.getSqlStatementContext())) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    @SuppressWarnings("unchecked")
    private boolean match(final TrafficAlgorithm trafficAlgorithm, final SQLStatementContext<?> statementContext) {
        if (trafficAlgorithm instanceof HintTrafficAlgorithm) {
            HintTrafficAlgorithm<Comparable<?>> hintTrafficAlgorithm = (HintTrafficAlgorithm<Comparable<?>>) trafficAlgorithm;
            for (HintTrafficValue<Comparable<?>> each : getHintTrafficValues(statementContext)) {
                if (hintTrafficAlgorithm.match(each)) {
                    return true;
                }
            }
        }
        if (trafficAlgorithm instanceof SegmentTrafficAlgorithm) {
            SegmentTrafficAlgorithm segmentTrafficAlgorithm = (SegmentTrafficAlgorithm) trafficAlgorithm;
            segmentTrafficAlgorithm.match(getSegmentTrafficValue(statementContext));
        }
        return false;
    }
    
    private Collection<HintTrafficValue<Comparable<?>>> getHintTrafficValues(final SQLStatementContext<?> statementContext) {
        Collection<HintTrafficValue<Comparable<?>>> result = new LinkedList<>();
        if (statementContext.getSqlStatement() instanceof AbstractSQLStatement) {
            for (CommentSegment each : ((AbstractSQLStatement) statementContext.getSqlStatement()).getCommentSegments()) {
                result.add(new HintTrafficValue<>(each.getText()));
            }
        }
        return result;
    }
    
    private SegmentTrafficValue getSegmentTrafficValue(final SQLStatementContext<?> statementContext) {
        Collection<SQLSegment> sqlSegments = statementContext instanceof SegmentAvailable ? ((SegmentAvailable) statementContext).getAllSegments() : Collections.emptyList();
        return new SegmentTrafficValue(statementContext.getSqlStatement(), sqlSegments);
    }
    
    /**
     * Find load balancer.
     * 
     * @param loadBalancerName load balancer name
     * @return load balancer
     */
    public TrafficLoadBalanceAlgorithm findLoadBalancer(final String loadBalancerName) {
        TrafficLoadBalanceAlgorithm loadBalanceAlgorithm = loadBalancers.get(loadBalancerName);
        Preconditions.checkState(null != loadBalanceAlgorithm, "Traffic load balance algorithm can not be null.");
        return loadBalanceAlgorithm;
    }
}
