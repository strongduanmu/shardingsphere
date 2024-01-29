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

package org.apache.shardingsphere.sharding.distsql.handler.update;

import com.google.common.base.Strings;
import lombok.Setter;
import org.apache.shardingsphere.distsql.handler.exception.algorithm.AlgorithmInUsedException;
import org.apache.shardingsphere.distsql.handler.exception.algorithm.MissingRequiredAlgorithmException;
import org.apache.shardingsphere.distsql.handler.type.rdl.rule.spi.database.DatabaseRuleDropExecutor;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.keygen.KeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.sharding.distsql.statement.DropShardingKeyGeneratorStatement;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Drop sharding key generator executor.
 */
@Setter
public final class DropShardingKeyGeneratorExecutor implements DatabaseRuleDropExecutor<DropShardingKeyGeneratorStatement, ShardingRuleConfiguration> {
    
    private ShardingSphereDatabase database;
    
    @Override
    public void checkBeforeUpdate(final DropShardingKeyGeneratorStatement sqlStatement, final ShardingRuleConfiguration currentRuleConfig) {
        if (null == currentRuleConfig && sqlStatement.isIfExists()) {
            return;
        }
        Collection<String> keyGeneratorNames = new LinkedList<>(sqlStatement.getNames());
        checkExist(keyGeneratorNames, currentRuleConfig, sqlStatement);
        checkInUsed(keyGeneratorNames, currentRuleConfig);
    }
    
    private void checkExist(final Collection<String> keyGeneratorNames, final ShardingRuleConfiguration currentRuleConfig, final DropShardingKeyGeneratorStatement sqlStatement) {
        if (sqlStatement.isIfExists()) {
            return;
        }
        Collection<String> notExistKeyGenerators = keyGeneratorNames.stream().filter(each -> !currentRuleConfig.getKeyGenerators().containsKey(each)).collect(Collectors.toList());
        ShardingSpherePreconditions.checkState(notExistKeyGenerators.isEmpty(), () -> new MissingRequiredAlgorithmException("Key generator", database.getName(), notExistKeyGenerators));
    }
    
    private void checkInUsed(final Collection<String> keyGeneratorNames, final ShardingRuleConfiguration currentRuleConfig) {
        Collection<String> usedKeyGenerators = getUsedKeyGenerators(currentRuleConfig);
        Collection<String> inUsedNames = keyGeneratorNames.stream().filter(usedKeyGenerators::contains).collect(Collectors.toList());
        ShardingSpherePreconditions.checkState(inUsedNames.isEmpty(), () -> new AlgorithmInUsedException("Key generator", database.getName(), inUsedNames));
    }
    
    private Collection<String> getUsedKeyGenerators(final ShardingRuleConfiguration shardingRuleConfig) {
        Collection<String> result = new LinkedHashSet<>();
        shardingRuleConfig.getTables().stream().filter(each -> null != each.getKeyGenerateStrategy()).forEach(each -> result.add(each.getKeyGenerateStrategy().getKeyGeneratorName()));
        shardingRuleConfig.getAutoTables().stream().filter(each -> null != each.getKeyGenerateStrategy()).forEach(each -> result.add(each.getKeyGenerateStrategy().getKeyGeneratorName()));
        KeyGenerateStrategyConfiguration keyGenerateStrategy = shardingRuleConfig.getDefaultKeyGenerateStrategy();
        if (null != keyGenerateStrategy && !Strings.isNullOrEmpty(keyGenerateStrategy.getKeyGeneratorName())) {
            result.add(keyGenerateStrategy.getKeyGeneratorName());
        }
        return result;
    }
    
    @Override
    public ShardingRuleConfiguration buildToBeDroppedRuleConfiguration(final DropShardingKeyGeneratorStatement sqlStatement, final ShardingRuleConfiguration currentRuleConfig) {
        ShardingRuleConfiguration result = new ShardingRuleConfiguration();
        for (String each : sqlStatement.getNames()) {
            result.getKeyGenerators().put(each, currentRuleConfig.getKeyGenerators().get(each));
        }
        return result;
    }
    
    @Override
    public boolean updateCurrentRuleConfiguration(final DropShardingKeyGeneratorStatement sqlStatement, final ShardingRuleConfiguration currentRuleConfig) {
        currentRuleConfig.getKeyGenerators().keySet().removeIf(sqlStatement.getNames()::contains);
        return false;
    }
    
    @Override
    public boolean hasAnyOneToBeDropped(final DropShardingKeyGeneratorStatement sqlStatement, final ShardingRuleConfiguration currentRuleConfig) {
        return null != currentRuleConfig && !getIdenticalData(currentRuleConfig.getKeyGenerators().keySet(), sqlStatement.getNames()).isEmpty();
    }
    
    @Override
    public Class<ShardingRuleConfiguration> getRuleConfigurationClass() {
        return ShardingRuleConfiguration.class;
    }
    
    @Override
    public Class<DropShardingKeyGeneratorStatement> getType() {
        return DropShardingKeyGeneratorStatement.class;
    }
}
