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

package org.apache.shardingsphere.infra.executor.sql.federate.schema.row;

import org.apache.calcite.DataContext;
import org.apache.calcite.rex.RexNode;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.infra.exception.ShardingSphereException;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.federate.schema.table.generator.FederateExecutionContextGenerator;
import org.apache.shardingsphere.infra.executor.sql.federate.schema.table.generator.FederateExecutionSQLGenerator;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.process.ExecuteProcessEngine;
import org.apache.shardingsphere.infra.optimize.core.metadata.FederateTableMetadata;
import org.apache.shardingsphere.sql.parser.sql.common.constant.QuoteCharacter;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.column.ColumnSegment;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Federate row executor.
 */
public final class FederateRowExecutor {
    
    private final ConfigurationProperties props;
    
    private final JDBCExecutor jdbcExecutor;
    
    private final ExecutionContext routeExecutionContext;
    
    private final JDBCExecutorCallback<? extends ExecuteResult> callback;
    
    private final DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine;
    
    private final Map<String, List<ColumnSegment>> tableColumns;
    
    public FederateRowExecutor(final ConfigurationProperties props, final JDBCExecutor jdbcExecutor, final ExecutionContext routeExecutionContext, 
                               final JDBCExecutorCallback<? extends ExecuteResult> callback, final DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine) {
        this.props = props;
        this.jdbcExecutor = jdbcExecutor;
        this.routeExecutionContext = routeExecutionContext;
        this.callback = callback;
        this.prepareEngine = prepareEngine;
        tableColumns = getTableColumns();
    }
    
    private Map<String, List<ColumnSegment>> getTableColumns() {
        if (routeExecutionContext.getSqlStatementContext() instanceof SelectStatementContext) {
            return ((SelectStatementContext) routeExecutionContext.getSqlStatementContext()).getTableColumns();
        }
        return Collections.emptyMap();
    }
    
    /**
     * Execute.
     *
     * @param metadata metadata
     * @param root root
     * @param filters filter
     * @param projects projects
     * @return a query result list
     */
    public Collection<QueryResult> execute(final FederateTableMetadata metadata, final DataContext root, final List<RexNode> filters, final int[] projects) {
        FederateExecutionContextGenerator generator = new FederateExecutionContextGenerator(metadata.getName(), routeExecutionContext, 
                new FederateExecutionSQLGenerator(root, filters, projects, getColumnNamesWithQuoteCharacter(metadata)));
        return execute(generator.generate());
    }
    
    private Collection<QueryResult> execute(final ExecutionContext context) {
        try {
            ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext = prepareEngine.prepare(context.getRouteContext(), context.getExecutionUnits());
            ExecuteProcessEngine.initialize(context.getLogicSQL(), executionGroupContext, props);
            Collection<QueryResult> result = jdbcExecutor.execute(executionGroupContext, callback).stream().map(each -> (QueryResult) each).collect(Collectors.toList());
            ExecuteProcessEngine.finish(executionGroupContext.getExecutionID());
            return result;
        } catch (final SQLException ex) {
            throw new ShardingSphereException(ex);
        } finally {
            ExecuteProcessEngine.clean();
        }
    }
    
    private List<String> getColumnNamesWithQuoteCharacter(final FederateTableMetadata metadata) {
        if (!tableColumns.containsKey(metadata.getName())) {
            return metadata.getColumnNames();
        }
        Map<String, QuoteCharacter> columnQuoteCharacters = tableColumns.get(metadata.getName()).stream()
                .collect(Collectors.toMap(each -> each.getIdentifier().getValue(), each -> each.getIdentifier().getQuoteCharacter(), (oldValue, currentValue) -> oldValue));
        return metadata.getColumnNames().stream().map(each -> columnQuoteCharacters.getOrDefault(each, QuoteCharacter.NONE).wrap(each)).collect(Collectors.toList());
    }
}
