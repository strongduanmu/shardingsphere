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

package org.apache.shardingsphere.mode.metadata;

import lombok.Getter;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.config.schema.SchemaConfiguration;
import org.apache.shardingsphere.infra.config.schema.impl.DataSourceProvidedSchemaConfiguration;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.executor.kernel.ExecutorEngine;
import org.apache.shardingsphere.infra.federation.optimizer.context.OptimizerContextFactory;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.rule.ShardingSphereRuleMetaData;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.schema.builder.spi.DialectSystemSchemaBuilder;
import org.apache.shardingsphere.infra.metadata.schema.loader.SchemaLoader;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.global.GlobalRulesBuilder;
import org.apache.shardingsphere.infra.rule.builder.schema.SchemaRulesBuilder;
import org.apache.shardingsphere.mode.metadata.persist.MetaDataPersistService;
import org.apache.shardingsphere.spi.singleton.SingletonSPIRegistry;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

/**
 * Meta data contexts builder.
 */
public final class MetaDataContextsBuilder {
    
    private static final Map<String, DialectSystemSchemaBuilder> DIALECT_SYSTEM_SCHEMA_BUILDERS 
            = SingletonSPIRegistry.getSingletonInstancesMap(DialectSystemSchemaBuilder.class, DialectSystemSchemaBuilder::getDatabaseType);
    
    private final Map<String, SchemaConfiguration> schemaConfigMap = new LinkedHashMap<>();
    
    private final Map<String, Collection<ShardingSphereRule>> schemaRulesMap = new LinkedHashMap<>();
    
    @Getter
    private final Map<String, ShardingSphereSchema> schemaMap = new LinkedHashMap<>();
    
    private final Collection<RuleConfiguration> globalRuleConfigs;
    
    private final ConfigurationProperties props;
    
    private final ExecutorEngine executorEngine;
    
    public MetaDataContextsBuilder(final Collection<RuleConfiguration> globalRuleConfigs, final Properties props) {
        this.globalRuleConfigs = globalRuleConfigs;
        this.props = new ConfigurationProperties(props);
        executorEngine = new ExecutorEngine(this.props.<Integer>getValue(ConfigurationPropertyKey.KERNEL_EXECUTOR_SIZE));
    }
    
    /**
     * Add schema information.
     * 
     * @param schemaName schema name
     * @param schemaConfig schema configuration
     * @param props properties
     * @throws SQLException SQL exception
     */
    public void addSchema(final String schemaName, final SchemaConfiguration schemaConfig, final Properties props) throws SQLException {
        Collection<ShardingSphereRule> schemaRules = getSchemaRules(schemaName, schemaConfig, props);
        ShardingSphereSchema schema = SchemaLoader.load(schemaConfig.getDataSources(), schemaRules, props);
        schemaConfigMap.put(schemaName, schemaConfig);
        schemaRulesMap.put(schemaName, schemaRules);
        schemaMap.put(schemaName, schema);
    }
    
    /**
     * Add system schema information.
     *
     * @param databaseType database type
     */
    public void addSystemSchemas(final DatabaseType databaseType) {
        findDialectSystemSchemaBuilder(databaseType).ifPresent(optional -> schemaMap.putAll(optional.build()));
    }
    
    private static Optional<DialectSystemSchemaBuilder> findDialectSystemSchemaBuilder(final DatabaseType databaseType) {
        return Optional.ofNullable(DIALECT_SYSTEM_SCHEMA_BUILDERS.get(databaseType.getName()));
    }
    
    private Collection<ShardingSphereRule> getSchemaRules(final String schemaName, final SchemaConfiguration schemaConfig, final Properties props) {
        return SchemaRulesBuilder.buildRules(schemaName, schemaConfig, new ConfigurationProperties(props));
    }
    
    /**
     * Build meta data contexts.
     * 
     * @param metaDataPersistService persist service
     * @exception SQLException SQL exception
     * @return meta data contexts
     */
    public MetaDataContexts build(final MetaDataPersistService metaDataPersistService) throws SQLException {
        Map<String, ShardingSphereMetaData> metaDataMap = getMetaDataMap();
        ShardingSphereRuleMetaData globalMetaData = new ShardingSphereRuleMetaData(globalRuleConfigs, GlobalRulesBuilder.buildRules(globalRuleConfigs, metaDataMap));
        return new MetaDataContexts(metaDataPersistService, metaDataMap, globalMetaData, executorEngine, OptimizerContextFactory.create(metaDataMap, globalMetaData), props);
    }
    
    private Map<String, ShardingSphereMetaData> getMetaDataMap() throws SQLException {
        Map<String, ShardingSphereMetaData> result = new HashMap<>(schemaMap.size(), 1);
        for (Entry<String, ShardingSphereSchema> entry : schemaMap.entrySet()) {
            String schemaName = entry.getKey();
            SchemaConfiguration schemaConfig = schemaConfigMap.getOrDefault(schemaName, new DataSourceProvidedSchemaConfiguration(Collections.emptyMap(), Collections.emptyList()));
            Collection<ShardingSphereRule> rules = schemaRulesMap.getOrDefault(schemaName, Collections.emptyList());
            result.put(schemaName, ShardingSphereMetaData.create(schemaName, entry.getValue(), schemaConfig, rules));
        }
        return result;
    }
}
