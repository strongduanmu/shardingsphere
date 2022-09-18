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

package org.apache.shardingsphere.sqlfederation.advanced;

import com.google.common.base.Preconditions;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.rule.ShardingSphereRuleMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.util.eventbus.EventBusContext;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sqlfederation.SQLFederationDataContext;
import org.apache.shardingsphere.sqlfederation.advanced.resultset.SQLFederationResultSet;
import org.apache.shardingsphere.sqlfederation.executor.FilterableTableScanExecutor;
import org.apache.shardingsphere.sqlfederation.executor.TableScanExecutorContext;
import org.apache.shardingsphere.sqlfederation.optimizer.ShardingSphereOptimizer;
import org.apache.shardingsphere.sqlfederation.optimizer.context.OptimizerContext;
import org.apache.shardingsphere.sqlfederation.optimizer.context.OptimizerContextFactory;
import org.apache.shardingsphere.sqlfederation.optimizer.context.parser.OptimizerParserContext;
import org.apache.shardingsphere.sqlfederation.optimizer.context.planner.OptimizerPlannerContextFactory;
import org.apache.shardingsphere.sqlfederation.optimizer.converter.SQLNodeConverterEngine;
import org.apache.shardingsphere.sqlfederation.optimizer.executor.TableScanExecutor;
import org.apache.shardingsphere.sqlfederation.optimizer.metadata.filter.FilterableSchema;
import org.apache.shardingsphere.sqlfederation.optimizer.planner.QueryOptimizePlannerFactory;
import org.apache.shardingsphere.sqlfederation.spi.SQLFederationExecutor;
import org.apache.shardingsphere.sqlfederation.spi.SQLFederationExecutorContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Advanced sql federation executor.
 */
public final class AdvancedSQLFederationExecutor implements SQLFederationExecutor {
    
    private String databaseName;
    
    private String schemaName;
    
    private OptimizerContext optimizerContext;
    
    private ShardingSphereRuleMetaData globalRuleMetaData;
    
    private ConfigurationProperties props;
    
    private JDBCExecutor jdbcExecutor;
    
    private EventBusContext eventBusContext;
    
    private ResultSet resultSet;
    
    private List<RelDataTypeField> fields;
    
    @Override
    public void init(final String databaseName, final String schemaName, final ShardingSphereMetaData metaData, final JDBCExecutor jdbcExecutor, final EventBusContext eventBusContext) {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.optimizerContext = OptimizerContextFactory.create(metaData.getDatabases(), metaData.getGlobalRuleMetaData());
        this.globalRuleMetaData = metaData.getGlobalRuleMetaData();
        this.props = metaData.getProps();
        this.jdbcExecutor = jdbcExecutor;
        this.eventBusContext = eventBusContext;
    }
    
    @Override
    public ResultSet executeQuery(final DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine,
                                  final JDBCExecutorCallback<? extends ExecuteResult> callback, final SQLFederationExecutorContext federationContext) {
        SQLStatementContext<?> sqlStatementContext = federationContext.getQueryContext().getSqlStatementContext();
        Preconditions.checkArgument(sqlStatementContext instanceof SelectStatementContext, "SQL statement context must be select statement context.");
        ShardingSphereSchema schema = federationContext.getDatabases().get(databaseName.toLowerCase()).getSchema(schemaName);
        AbstractSchema sqlFederationSchema = createSQLFederationSchema(prepareEngine, schema, callback, federationContext);
        Map<String, Object> parameters = createParameters(federationContext.getQueryContext().getParameters());
        Enumerator<Object> enumerator = execute(sqlStatementContext.getSqlStatement(), sqlFederationSchema, parameters).enumerator();
        resultSet = new SQLFederationResultSet(enumerator, schema, sqlFederationSchema, sqlStatementContext, fields);
        return resultSet;
    }
    
    private Map<String, Object> createParameters(final List<Object> parameters) {
        Map<String, Object> result = new HashMap<>(parameters.size(), 1);
        int index = 0;
        for (Object each : parameters) {
            result.put("?" + index++, each);
        }
        return result;
    }
    
    private AbstractSchema createSQLFederationSchema(final DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine, final ShardingSphereSchema schema,
                                                     final JDBCExecutorCallback<? extends ExecuteResult> callback, final SQLFederationExecutorContext federationContext) {
        TableScanExecutorContext executorContext = new TableScanExecutorContext(databaseName, schemaName, props, federationContext);
        // TODO replace FilterableTableScanExecutor with TranslatableTableScanExecutor
        TableScanExecutor executor = new FilterableTableScanExecutor(prepareEngine, jdbcExecutor, callback, optimizerContext, globalRuleMetaData, executorContext, eventBusContext);
        // TODO replace FilterableSchema with TranslatableSchema
        return new FilterableSchema(schemaName, schema, executor);
    }
    
    @SuppressWarnings("unchecked")
    private Enumerable<Object> execute(final SQLStatement sqlStatement, final AbstractSchema sqlFederationSchema, final Map<String, Object> parameters) {
        OptimizerParserContext parserContext = optimizerContext.getParserContexts().get(databaseName);
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(parserContext.getDialectProps());
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();
        CalciteCatalogReader catalogReader = OptimizerPlannerContextFactory.createCatalogReader(schemaName, sqlFederationSchema, relDataTypeFactory, connectionConfig);
        SqlValidator validator = OptimizerPlannerContextFactory.createValidator(catalogReader, relDataTypeFactory, parserContext.getDatabaseType(), connectionConfig);
        SqlToRelConverter converter = OptimizerPlannerContextFactory.createConverter(catalogReader, validator, relDataTypeFactory);
        SqlNode sqlNode = SQLNodeConverterEngine.convert(sqlStatement);
        RelNode logicPlan = converter.convertQuery(sqlNode, true, true).rel;
        RelDataType validatedNodeType = validator.getValidatedNodeType(sqlNode);
        fields = validatedNodeType.getFieldList();
        // TODO get dataType and dataType name from —— validatedNodeType.getFieldList().get(0).getType().getSqlTypeName().getJdbcOrdinal()
        RelNode bestPlan =
                new ShardingSphereOptimizer(converter, QueryOptimizePlannerFactory.createHepPlanner()).optimize(logicPlan);
        Bindable<Object> executablePlan = EnumerableInterpretable.toBindable(Collections.emptyMap(), null, (EnumerableRel) bestPlan, EnumerableRel.Prefer.ARRAY);
        return executablePlan.bind(new SQLFederationDataContext(validator, converter, parameters));
    }
    
    @Override
    public ResultSet getResultSet() {
        return resultSet;
    }
    
    @Override
    public void close() throws SQLException {
        if (null != resultSet) {
            resultSet.close();
        }
    }
    
    @Override
    public String getType() {
        return "ADVANCED";
    }
}
