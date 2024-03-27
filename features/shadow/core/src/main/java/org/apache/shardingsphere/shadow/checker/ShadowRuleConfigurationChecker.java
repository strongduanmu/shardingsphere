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

package org.apache.shardingsphere.shadow.checker;

import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.EmptyAlgorithmException;
import org.apache.shardingsphere.infra.config.rule.checker.RuleConfigurationChecker;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.core.external.sql.identifier.SQLExceptionIdentifier;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.shadow.api.config.ShadowRuleConfiguration;
import org.apache.shardingsphere.shadow.api.config.datasource.ShadowDataSourceConfiguration;
import org.apache.shardingsphere.shadow.api.config.table.ShadowTableConfiguration;
import org.apache.shardingsphere.shadow.constant.ShadowOrder;
import org.apache.shardingsphere.shadow.exception.algorithm.NotImplementHintShadowAlgorithmException;
import org.apache.shardingsphere.shadow.exception.metadata.MissingRequiredShadowConfigurationException;
import org.apache.shardingsphere.shadow.exception.metadata.ShadowDataSourceMappingNotFoundException;
import org.apache.shardingsphere.shadow.spi.ShadowAlgorithm;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shadow rule configuration checker.
 */
public final class ShadowRuleConfigurationChecker implements RuleConfigurationChecker<ShadowRuleConfiguration> {
    
    @Override
    public void check(final String databaseName, final ShadowRuleConfiguration ruleConfig, final Map<String, DataSource> dataSourceMap, final Collection<ShardingSphereRule> builtRules) {
        checkShadowAlgorithms(ruleConfig.getShadowAlgorithms());
        checkDefaultShadowAlgorithmConfiguration(ruleConfig.getDefaultShadowAlgorithmName(), ruleConfig.getShadowAlgorithms());
        checkDataSources(ruleConfig.getDataSources(), dataSourceMap, databaseName);
        // TODO move setDefaultShadowDataSource to yaml swapper
        setDefaultShadowDataSource(ruleConfig.getTables(), ruleConfig.getDataSources());
        checkShadowTableDataSourcesReferences(ruleConfig.getTables(), ruleConfig.getDataSources());
        // TODO move setDefaultShadowAlgorithm to yaml swapper
        setDefaultShadowAlgorithm(ruleConfig.getTables(), ruleConfig.getDefaultShadowAlgorithmName());
        checkShadowTableAlgorithmsReferences(ruleConfig.getTables(), ruleConfig.getShadowAlgorithms(), databaseName);
    }
    
    private void checkShadowAlgorithms(final Map<String, AlgorithmConfiguration> shadowAlgorithmConfigs) {
        shadowAlgorithmConfigs.values().forEach(each -> TypedSPILoader.checkService(ShadowAlgorithm.class, each.getType(), each.getProps()));
    }
    
    private void checkDataSources(final Collection<ShadowDataSourceConfiguration> shadowDataSources, final Map<String, DataSource> dataSourceMap, final String databaseName) {
        for (ShadowDataSourceConfiguration each : shadowDataSources) {
            ShardingSpherePreconditions.checkState(dataSourceMap.containsKey(each.getProductionDataSourceName()),
                    () -> new MissingRequiredShadowConfigurationException("ProductionDataSourceName", databaseName));
            ShardingSpherePreconditions.checkState(dataSourceMap.containsKey(each.getShadowDataSourceName()),
                    () -> new MissingRequiredShadowConfigurationException("ShadowDataSourceName", databaseName));
        }
    }
    
    private void setDefaultShadowDataSource(final Map<String, ShadowTableConfiguration> shadowTables, final Collection<ShadowDataSourceConfiguration> shadowDataSources) {
        if (1 == shadowDataSources.size()) {
            for (ShadowTableConfiguration each : shadowTables.values()) {
                if (each.getDataSourceNames().isEmpty()) {
                    each.getDataSourceNames().add(shadowDataSources.iterator().next().getName());
                }
            }
        }
    }
    
    private void checkShadowTableDataSourcesReferences(final Map<String, ShadowTableConfiguration> shadowTables, final Collection<ShadowDataSourceConfiguration> shadowDataSources) {
        Collection<String> dataSourceNames = shadowDataSources.stream().map(ShadowDataSourceConfiguration::getName).collect(Collectors.toSet());
        shadowTables.forEach((key, value) -> {
            for (String each : value.getDataSourceNames()) {
                ShardingSpherePreconditions.checkState(dataSourceNames.contains(each), () -> new ShadowDataSourceMappingNotFoundException(key));
            }
        });
    }
    
    private void checkDefaultShadowAlgorithmConfiguration(final String defaultShadowAlgorithmName, final Map<String, AlgorithmConfiguration> shadowAlgorithmConfigs) {
        if (null != defaultShadowAlgorithmName) {
            AlgorithmConfiguration algorithmConfig = shadowAlgorithmConfigs.get(defaultShadowAlgorithmName);
            ShardingSpherePreconditions.checkState(null != algorithmConfig && "SQL_HINT".equals(algorithmConfig.getType()), NotImplementHintShadowAlgorithmException::new);
        }
    }
    
    private void setDefaultShadowAlgorithm(final Map<String, ShadowTableConfiguration> shadowTables, final String defaultShadowAlgorithmName) {
        for (ShadowTableConfiguration each : shadowTables.values()) {
            Collection<String> shadowAlgorithmNames = each.getShadowAlgorithmNames();
            if (null != defaultShadowAlgorithmName && shadowAlgorithmNames.isEmpty()) {
                shadowAlgorithmNames.add(defaultShadowAlgorithmName);
            }
        }
    }
    
    private void checkShadowTableAlgorithmsReferences(final Map<String, ShadowTableConfiguration> shadowTables, final Map<String, AlgorithmConfiguration> shadowAlgorithms, final String databaseName) {
        for (ShadowTableConfiguration each : shadowTables.values()) {
            ShardingSpherePreconditions.checkState(!each.getShadowAlgorithmNames().isEmpty(), () -> new EmptyAlgorithmException("Shadow", new SQLExceptionIdentifier(databaseName)));
            each.getShadowAlgorithmNames().forEach(shadowAlgorithmName -> ShardingSpherePreconditions.checkState(shadowAlgorithms.containsKey(shadowAlgorithmName),
                    () -> new MissingRequiredShadowConfigurationException("ShadowAlgorithmName", databaseName)));
        }
    }
    
    @Override
    public Collection<String> getRequiredDataSourceNames(final ShadowRuleConfiguration ruleConfig) {
        Collection<String> result = new LinkedHashSet<>();
        ruleConfig.getDataSources().forEach(each -> {
            if (null != each.getShadowDataSourceName()) {
                result.add(each.getShadowDataSourceName());
            }
            if (null != each.getProductionDataSourceName()) {
                result.add(each.getProductionDataSourceName());
            }
        });
        return result;
    }
    
    @Override
    public int getOrder() {
        return ShadowOrder.ORDER;
    }
    
    @Override
    public Class<ShadowRuleConfiguration> getTypeClass() {
        return ShadowRuleConfiguration.class;
    }
}
