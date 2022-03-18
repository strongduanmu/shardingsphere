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

package org.apache.shardingsphere.mode.manager.cluster;

import com.google.common.base.Preconditions;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.infra.config.schema.SchemaConfiguration;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.config.schema.impl.DataSourceProvidedSchemaConfiguration;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRecognizer;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.instance.InstanceContext;
import org.apache.shardingsphere.infra.instance.definition.InstanceDefinition;
import org.apache.shardingsphere.infra.instance.definition.InstanceType;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.mode.manager.ContextManagerBuilder;
import org.apache.shardingsphere.mode.manager.ContextManagerBuilderParameter;
import org.apache.shardingsphere.mode.manager.cluster.coordinator.ClusterContextManagerCoordinator;
import org.apache.shardingsphere.mode.manager.cluster.coordinator.RegistryCenter;
import org.apache.shardingsphere.mode.manager.cluster.coordinator.registry.workerid.generator.ClusterWorkerIdGenerator;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.mode.metadata.MetaDataContextsBuilder;
import org.apache.shardingsphere.mode.metadata.persist.MetaDataPersistService;
import org.apache.shardingsphere.mode.repository.cluster.ClusterPersistRepository;
import org.apache.shardingsphere.mode.repository.cluster.ClusterPersistRepositoryConfiguration;
import org.apache.shardingsphere.mode.repository.cluster.ClusterPersistRepositoryFactory;
import org.apache.shardingsphere.schedule.core.api.ModeScheduleContextFactory;
import org.apache.shardingsphere.transaction.config.TransactionRuleConfiguration;
import org.apache.shardingsphere.transaction.context.TransactionContexts;
import org.apache.shardingsphere.transaction.context.TransactionContextsBuilder;
import org.apache.shardingsphere.transaction.rule.TransactionRule;
import org.apache.shardingsphere.transaction.spi.TransactionConfigurationFileGenerator;
import org.apache.shardingsphere.transaction.spi.TransactionConfigurationFileGeneratorFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

/**
 * Cluster context manager builder.
 */
public final class ClusterContextManagerBuilder implements ContextManagerBuilder {
    
    private static final String DEFAULT_FRONTEND_DATABASE_PROTOCOL_TYPE = "MySQL";
    
    @Override
    public ContextManager build(final ContextManagerBuilderParameter parameter) throws SQLException {
        ModeScheduleContextFactory.getInstance().init(parameter.getInstanceDefinition().getInstanceId().getId(), parameter.getModeConfig());
        ClusterPersistRepository repository = ClusterPersistRepositoryFactory.newInstance((ClusterPersistRepositoryConfiguration) parameter.getModeConfig().getRepository());
        MetaDataPersistService metaDataPersistService = new MetaDataPersistService(repository);
        persistConfigurations(metaDataPersistService, parameter);
        RegistryCenter registryCenter = new RegistryCenter(repository);
        MetaDataContextsBuilder metaDataContextsBuilder = createMetaDataContextsBuilder(metaDataPersistService, parameter);
        persistMetaData(metaDataPersistService, metaDataContextsBuilder.getSchemaMap());
        MetaDataContexts metaDataContexts = metaDataContextsBuilder.build(metaDataPersistService);
        Properties transactionProps = getTransactionProperties(metaDataContexts);
        persistTransactionConfiguration(parameter, metaDataPersistService, transactionProps);
        ContextManager result = createContextManager(repository, metaDataPersistService, parameter.getInstanceDefinition(), metaDataContexts, transactionProps, parameter.getModeConfig());
        registerOnline(metaDataPersistService, parameter.getInstanceDefinition(), result, registryCenter);
        return result;
    }
    
    private Properties getTransactionProperties(final MetaDataContexts metaDataContexts) {
        Optional<String> schemaName = metaDataContexts.getAllSchemaNames().stream().findFirst();
        Optional<TransactionRule> transactionRule =
                metaDataContexts.getGlobalRuleMetaData().getRules().stream().filter(each -> each instanceof TransactionRule).map(each -> (TransactionRule) each).findFirst();
        Optional<TransactionConfigurationFileGenerator> fileGenerator = transactionRule.isPresent()
                ? TransactionConfigurationFileGeneratorFactory.newInstance(transactionRule.get().getProviderType()) : Optional.empty();
        if (!schemaName.isPresent() || !fileGenerator.isPresent()) {
            return transactionRule.isPresent() ? transactionRule.get().getProps() : new Properties();
        }
        ShardingSphereMetaData metaData = metaDataContexts.getMetaData(schemaName.get());
        Properties result = fileGenerator.get().getTransactionProps(transactionRule.get().getProps(),
                new DataSourceProvidedSchemaConfiguration(metaData.getResource().getDataSources(), metaData.getRuleMetaData().getConfigurations()), getType());
        Optional<TransactionRuleConfiguration> transactionRuleConfig = metaDataContexts.getGlobalRuleMetaData().findSingleRuleConfiguration(TransactionRuleConfiguration.class);
        Preconditions.checkState(transactionRuleConfig.isPresent());
        transactionRuleConfig.get().getProps().clear();
        transactionRuleConfig.get().getProps().putAll(result);
        transactionRule.get().getProps().clear();
        transactionRule.get().getProps().putAll(result);
        return result;
    }
    
    private void persistTransactionConfiguration(final ContextManagerBuilderParameter parameter, final MetaDataPersistService metaDataPersistService, final Properties transactionProps) {
        if (!transactionProps.isEmpty()) {
            metaDataPersistService.persistTransactionRule(transactionProps, true);
        }
        String instanceId = parameter.getInstanceDefinition().getInstanceId().getId();
        if (!metaDataPersistService.getComputeNodePersistService().loadXaRecoveryId(instanceId).isPresent()) {
            metaDataPersistService.getComputeNodePersistService().persistInstanceXaRecoveryId(instanceId, instanceId);
        }
    }
    
    private MetaDataContextsBuilder createMetaDataContextsBuilder(final MetaDataPersistService metaDataPersistService, final ContextManagerBuilderParameter parameter) throws SQLException {
        Collection<String> schemaNames = InstanceType.JDBC == parameter.getInstanceDefinition().getInstanceType()
                ? parameter.getSchemaConfigs().keySet() : metaDataPersistService.getSchemaMetaDataService().loadAllNames();
        Collection<RuleConfiguration> globalRuleConfigs = metaDataPersistService.getGlobalRuleService().load();
        Properties props = metaDataPersistService.getPropsService().load();
        MetaDataContextsBuilder result = new MetaDataContextsBuilder(globalRuleConfigs, props);
        DatabaseType databaseType = getDatabaseType(parameter);
        for (String each : schemaNames) {
            result.addSchema(each, createSchemaConfiguration(each, metaDataPersistService, parameter), props);
            if (!databaseType.getSystemSchemas().contains(each)) {
                Map<String, DataSource> dataSources = metaDataPersistService.getEffectiveDataSources(each, parameter.getSchemaConfigs());
                Collection<RuleConfiguration> schemaRuleConfigs = metaDataPersistService.getSchemaRuleService().load(each);
                result.addSchema(each, new DataSourceProvidedSchemaConfiguration(dataSources, schemaRuleConfigs), props);
            }
        }
        result.addSystemSchemas(databaseType);
        return result;
    }
    
    private SchemaConfiguration createSchemaConfiguration(final String schemaName, final MetaDataPersistService metaDataPersistService,
                                                          final ContextManagerBuilderParameter parameter) {
        Map<String, DataSource> dataSources = metaDataPersistService.getEffectiveDataSources(schemaName, parameter.getSchemaConfigs());
        Collection<RuleConfiguration> schemaRuleConfigs = metaDataPersistService.getSchemaRuleService().load(schemaName);
        return new DataSourceProvidedSchemaConfiguration(dataSources, schemaRuleConfigs);
    }
    
    /**
     * Get database type.
     *
     * @param parameter context manager builder parameter
     * @return database type
     */
    public static DatabaseType getDatabaseType(final ContextManagerBuilderParameter parameter) {
        Optional<DatabaseType> configuredDatabaseType = findConfiguredDatabaseType(new ConfigurationProperties(parameter.getProps()));
        if (configuredDatabaseType.isPresent()) {
            return configuredDatabaseType.get();
        }
        if (parameter.getSchemaConfigs().isEmpty()) {
            return DatabaseTypeRegistry.getTrunkDatabaseType(DEFAULT_FRONTEND_DATABASE_PROTOCOL_TYPE);
        }
        Optional<? extends SchemaConfiguration> schemaConfiguration = parameter.getSchemaConfigs()
                .values().stream().filter(each -> !each.getDataSources().isEmpty() && !each.getRuleConfigurations().isEmpty()).findFirst();
        if (!schemaConfiguration.isPresent()) {
            return DatabaseTypeRegistry.getTrunkDatabaseType(DEFAULT_FRONTEND_DATABASE_PROTOCOL_TYPE);
        }
        return DatabaseTypeRecognizer.getDatabaseType(schemaConfiguration.get().getDataSources().values());
    }
    
    private static Optional<DatabaseType> findConfiguredDatabaseType(final ConfigurationProperties configProps) {
        String configuredDatabaseType = configProps.getValue(ConfigurationPropertyKey.PROXY_FRONTEND_DATABASE_PROTOCOL_TYPE);
        return configuredDatabaseType.isEmpty() ? Optional.empty() : Optional.of(DatabaseTypeRegistry.getTrunkDatabaseType(configuredDatabaseType));
    }
    
    private void persistConfigurations(final MetaDataPersistService metaDataPersistService, final ContextManagerBuilderParameter parameter) {
        boolean isOverwrite = parameter.getModeConfig().isOverwrite();
        if (!parameter.isEmpty()) {
            metaDataPersistService.persistConfigurations(parameter.getSchemaConfigs(), parameter.getGlobalRuleConfigs(), parameter.getProps(), isOverwrite);
        }
        metaDataPersistService.persistInstanceLabels(parameter.getInstanceDefinition().getInstanceId().getId(), parameter.getLabels(), isOverwrite);
    }
    
    private void persistMetaData(final MetaDataPersistService metaDataPersistService, final Map<String, ShardingSphereSchema> schemaMap) {
        for (Entry<String, ShardingSphereSchema> entry : schemaMap.entrySet()) {
            metaDataPersistService.getSchemaMetaDataService().persist(entry.getKey(), entry.getValue());
        }
    }
    
    private ContextManager createContextManager(final ClusterPersistRepository repository, final MetaDataPersistService metaDataPersistService,
                                                final InstanceDefinition instanceDefinition, final MetaDataContexts metaDataContexts,
                                                final Properties transactionProps, final ModeConfiguration modeConfiguration) {
        InstanceContext instanceContext = new InstanceContext(metaDataPersistService.getComputeNodePersistService().loadComputeNodeInstance(instanceDefinition),
                new ClusterWorkerIdGenerator(repository, metaDataPersistService, instanceDefinition), modeConfiguration);
        generateTransactionConfigurationFile(instanceContext, metaDataContexts, transactionProps);
        TransactionContexts transactionContexts = new TransactionContextsBuilder(metaDataContexts.getMetaDataMap(), metaDataContexts.getGlobalRuleMetaData().getRules()).build();
        ContextManager result = new ContextManager();
        result.init(metaDataContexts, transactionContexts, instanceContext);
        return result;
    }
    
    private void generateTransactionConfigurationFile(final InstanceContext instanceContext, final MetaDataContexts metaDataContexts, final Properties transactionProps) {
        Optional<TransactionRule> transactionRule = metaDataContexts.getGlobalRuleMetaData().getRules()
                .stream().filter(each -> each instanceof TransactionRule).map(each -> (TransactionRule) each).findFirst();
        if (transactionRule.isPresent()) {
            Optional<TransactionConfigurationFileGenerator> fileGenerator = TransactionConfigurationFileGeneratorFactory.newInstance(transactionRule.get().getProviderType());
            fileGenerator.ifPresent(optional -> optional.generateFile(transactionProps, instanceContext));
        }
    }
    
    private void registerOnline(final MetaDataPersistService metaDataPersistService, final InstanceDefinition instanceDefinition, final ContextManager contextManager,
                                final RegistryCenter registryCenter) {
        new ClusterContextManagerCoordinator(metaDataPersistService, contextManager, registryCenter);
        registryCenter.onlineInstance(instanceDefinition);
        contextManager.getInstanceContext().getComputeNodeInstances().addAll(metaDataPersistService.getComputeNodePersistService().loadAllComputeNodeInstances());
    }
    
    @Override
    public String getType() {
        return "Cluster";
    }
}
