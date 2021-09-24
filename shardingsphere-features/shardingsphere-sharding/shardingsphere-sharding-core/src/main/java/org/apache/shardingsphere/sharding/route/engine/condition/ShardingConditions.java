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

package org.apache.shardingsphere.sharding.route.engine.condition;

import lombok.Getter;
import lombok.ToString;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.hint.HintManager;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.HintShardingStrategyConfiguration;
import org.apache.shardingsphere.sharding.route.engine.condition.value.ListShardingConditionValue;
import org.apache.shardingsphere.sharding.route.engine.condition.value.RangeShardingConditionValue;
import org.apache.shardingsphere.sharding.route.engine.condition.value.ShardingConditionValue;
import org.apache.shardingsphere.sharding.rule.BindingTableRule;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.apache.shardingsphere.sharding.rule.TableRule;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.subquery.SubquerySegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.common.util.SafeNumberOperationUtil;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Sharding conditions.
 */
@Getter
@ToString
public final class ShardingConditions {
    
    private final List<ShardingCondition> conditions;
    
    private final SQLStatementContext<?> sqlStatementContext;
    
    private final ShardingRule rule;
    
    private final boolean needMerge;
    
    public ShardingConditions(final List<ShardingCondition> conditions, final SQLStatementContext<?> sqlStatementContext, final ShardingRule rule) {
        this.conditions = conditions;
        this.sqlStatementContext = sqlStatementContext;
        this.rule = rule;
        needMerge = isNeedMerge(conditions, sqlStatementContext, rule);
    }
    
    /**
     * Judge sharding conditions is always false or not.
     *
     * @return sharding conditions is always false or not
     */
    public boolean isAlwaysFalse() {
        if (conditions.isEmpty()) {
            return false;
        }
        for (ShardingCondition each : conditions) {
            if (!(each instanceof AlwaysFalseShardingCondition)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Merge sharding conditions.
     */
    public void merge() {
        if (conditions.size() > 1) {
            ShardingCondition shardingCondition = conditions.remove(conditions.size() - 1);
            conditions.clear();
            conditions.add(shardingCondition);
        }
    }
    
    private boolean isNeedMerge(final List<ShardingCondition> conditions, final SQLStatementContext<?> sqlStatementContext, final ShardingRule rule) {
        Collection<SelectStatement> selectStatements = getSelectStatements(sqlStatementContext);
        if (selectStatements.size() > 1) {
            boolean containsShardingTable = !rule.getShardingLogicTableNames(sqlStatementContext.getTablesContext().getTableNames()).isEmpty();
            return containsShardingTable && isContainsShardingCondition(selectStatements, conditions) && isSameShardingCondition(rule, conditions);
        }
        return false;
    }
    
    private Collection<SelectStatement> getSelectStatements(final SQLStatementContext<?> sqlStatementContext) {
        Collection<SelectStatement> result = new LinkedList<>();
        if (sqlStatementContext instanceof SelectStatementContext) {
            result.add(((SelectStatementContext) sqlStatementContext).getSqlStatement());
            result.addAll(((SelectStatementContext) sqlStatementContext).getSubquerySegments().stream().map(SubquerySegment::getSelect).collect(Collectors.toList()));
        }
        if (sqlStatementContext instanceof InsertStatementContext && null != ((InsertStatementContext) sqlStatementContext).getInsertSelectContext()) {
            SelectStatementContext selectStatementContext = ((InsertStatementContext) sqlStatementContext).getInsertSelectContext().getSelectStatementContext();
            result.add(selectStatementContext.getSqlStatement());
            result.addAll(selectStatementContext.getSubquerySegments().stream().map(SubquerySegment::getSelect).collect(Collectors.toList()));
        }
        return result;
    }
    
    private boolean isContainsShardingCondition(final Collection<SelectStatement> selectStatements, final List<ShardingCondition> conditions) {
        Map<Integer, List<ShardingCondition>> startIndexShardingConditions = conditions.stream().collect(Collectors.groupingBy(ShardingCondition::getStartIndex));
        for (SelectStatement each : selectStatements) {
            if (!each.getWhere().isPresent() || !startIndexShardingConditions.containsKey(each.getWhere().get().getExpr().getStartIndex())) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Judge whether all sharding conditions are same or not.
     *
     * @return whether all sharding conditions are same or not
     */
    public boolean isSameShardingCondition() {
        for (String each : sqlStatementContext.getTablesContext().getTableNames()) {
            Optional<TableRule> tableRule = rule.findTableRule(each);
            if (tableRule.isPresent() && isRoutingByHint(rule, tableRule.get())
                    && !HintManager.getDatabaseShardingValues(each).isEmpty() && !HintManager.getTableShardingValues(each).isEmpty()) {
                return false;
            }
        }
        return conditions.size() == 1;
    }
    
    private boolean isSameShardingCondition(final ShardingRule shardingRule, final List<ShardingCondition> conditions) {
        ShardingCondition example = conditions.get(0);
        for (ShardingCondition each : conditions) {
            if (!isSameShardingCondition(shardingRule, example, each)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isSameShardingCondition(final ShardingRule shardingRule, final ShardingCondition shardingCondition1, final ShardingCondition shardingCondition2) {
        if (shardingCondition1.getValues().size() != shardingCondition2.getValues().size()) {
            return false;
        }
        for (int i = 0; i < shardingCondition1.getValues().size(); i++) {
            ShardingConditionValue shardingConditionValue1 = shardingCondition1.getValues().get(i);
            ShardingConditionValue shardingConditionValue2 = shardingCondition2.getValues().get(i);
            if (!isSameShardingConditionValue(shardingRule, shardingConditionValue1, shardingConditionValue2)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isRoutingByHint(final ShardingRule shardingRule, final TableRule tableRule) {
        return shardingRule.getDatabaseShardingStrategyConfiguration(tableRule) instanceof HintShardingStrategyConfiguration
                && shardingRule.getTableShardingStrategyConfiguration(tableRule) instanceof HintShardingStrategyConfiguration;
    }
    
    private boolean isSameShardingConditionValue(final ShardingRule shardingRule, final ShardingConditionValue shardingConditionValue1, final ShardingConditionValue shardingConditionValue2) {
        return isSameLogicTable(shardingRule, shardingConditionValue1, shardingConditionValue2) && shardingConditionValue1.getColumnName().equals(shardingConditionValue2.getColumnName())
                && isSameValue(shardingConditionValue1, shardingConditionValue2);
    }
    
    private boolean isSameLogicTable(final ShardingRule shardingRule, final ShardingConditionValue shardingValue1, final ShardingConditionValue shardingValue2) {
        return shardingValue1.getTableName().equals(shardingValue2.getTableName()) || isBindingTable(shardingRule, shardingValue1, shardingValue2);
    }
    
    private boolean isBindingTable(final ShardingRule shardingRule, final ShardingConditionValue shardingValue1, final ShardingConditionValue shardingValue2) {
        Optional<BindingTableRule> bindingRule = shardingRule.findBindingTableRule(shardingValue1.getTableName());
        return bindingRule.isPresent() && bindingRule.get().hasLogicTable(shardingValue2.getTableName());
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean isSameValue(final ShardingConditionValue shardingConditionValue1, final ShardingConditionValue shardingConditionValue2) {
        if (shardingConditionValue1 instanceof ListShardingConditionValue && shardingConditionValue2 instanceof ListShardingConditionValue) {
            return SafeNumberOperationUtil.safeCollectionEquals(
                    ((ListShardingConditionValue) shardingConditionValue1).getValues(), ((ListShardingConditionValue) shardingConditionValue2).getValues());
        } else if (shardingConditionValue1 instanceof RangeShardingConditionValue && shardingConditionValue2 instanceof RangeShardingConditionValue) {
            return SafeNumberOperationUtil.safeRangeEquals(
                    ((RangeShardingConditionValue) shardingConditionValue1).getValueRange(), ((RangeShardingConditionValue) shardingConditionValue2).getValueRange());
        }
        return false;
    }
}
