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

package org.apache.shardingsphere.proxy.backend.connector;

import lombok.Getter;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.dialect.exception.transaction.TableModifyInTransactionException;
import org.apache.shardingsphere.infra.executor.kernel.ExecutorEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupReportContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.RawExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.RawSQLExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.callback.RawSQLExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.StatementOption;
import org.apache.shardingsphere.infra.executor.sql.prepare.raw.RawExecutionPrepareEngine;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.attribute.raw.RawExecutionRuleAttribute;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;
import org.apache.shardingsphere.infra.session.connection.transaction.TransactionConnectionContext;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.infra.spi.ShardingSphereServiceLoader;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.proxy.backend.connector.jdbc.executor.ProxyJDBCExecutor;
import org.apache.shardingsphere.proxy.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.apache.shardingsphere.proxy.backend.connector.sane.SaneQueryResultEngine;
import org.apache.shardingsphere.proxy.backend.context.BackendExecutorContext;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.session.transaction.TransactionStatus;
import org.apache.shardingsphere.proxy.backend.util.TransactionUtils;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.CloseStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.DDLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.FetchStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.MoveStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.TruncateStatement;
import org.apache.shardingsphere.sql.parser.statement.mysql.dml.MySQLInsertStatement;
import org.apache.shardingsphere.sql.parser.statement.opengauss.OpenGaussStatement;
import org.apache.shardingsphere.sql.parser.statement.opengauss.ddl.OpenGaussCursorStatement;
import org.apache.shardingsphere.sql.parser.statement.postgresql.PostgreSQLStatement;
import org.apache.shardingsphere.sqlfederation.engine.SQLFederationEngine;
import org.apache.shardingsphere.transaction.api.TransactionType;
import org.apache.shardingsphere.transaction.spi.TransactionHook;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Proxy SQL Executor.
 */
public final class ProxySQLExecutor {
    
    private final String type;
    
    private final ProxyDatabaseConnectionManager databaseConnectionManager;
    
    private final ProxyJDBCExecutor regularExecutor;
    
    private final RawExecutor rawExecutor;
    
    @Getter
    private final SQLFederationEngine sqlFederationEngine;
    
    private final Collection<TransactionHook> transactionHooks = ShardingSphereServiceLoader.getServiceInstances(TransactionHook.class);
    
    public ProxySQLExecutor(final String type, final ProxyDatabaseConnectionManager databaseConnectionManager, final DatabaseConnector databaseConnector, final QueryContext queryContext) {
        this.type = type;
        this.databaseConnectionManager = databaseConnectionManager;
        ExecutorEngine executorEngine = BackendExecutorContext.getInstance().getExecutorEngine();
        ConnectionContext connectionContext = databaseConnectionManager.getConnectionSession().getConnectionContext();
        JDBCExecutor jdbcExecutor = new JDBCExecutor(executorEngine, connectionContext);
        regularExecutor = new ProxyJDBCExecutor(type, databaseConnectionManager.getConnectionSession(), databaseConnector, jdbcExecutor);
        rawExecutor = new RawExecutor(executorEngine, connectionContext);
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        String defaultDatabaseName = databaseConnectionManager.getConnectionSession().getDefaultDatabaseName();
        String defaultSchemaName = getSchemaName(queryContext.getSqlStatementContext(), metaDataContexts.getMetaData().getDatabase(defaultDatabaseName));
        sqlFederationEngine = new SQLFederationEngine(defaultDatabaseName, defaultSchemaName, metaDataContexts.getMetaData(), metaDataContexts.getStatistics(), jdbcExecutor);
    }
    
    private String getSchemaName(final SQLStatementContext sqlStatementContext, final ShardingSphereDatabase database) {
        String defaultSchemaName = new DatabaseTypeRegistry(sqlStatementContext.getDatabaseType()).getDefaultSchemaName(database.getName());
        return sqlStatementContext instanceof TableAvailable
                ? ((TableAvailable) sqlStatementContext).getTablesContext().getSchemaName().orElse(defaultSchemaName)
                : defaultSchemaName;
    }
    
    /**
     * Check execute prerequisites.
     *
     * @param executionContext execution context
     */
    public void checkExecutePrerequisites(final ExecutionContext executionContext) {
        ShardingSpherePreconditions.checkState(isValidExecutePrerequisites(executionContext), () -> new TableModifyInTransactionException(getTableName(executionContext)));
    }
    
    private boolean isValidExecutePrerequisites(final ExecutionContext executionContext) {
        return !isExecuteDDLInXATransaction(executionContext.getSqlStatementContext().getSqlStatement())
                && !isExecuteDDLInPostgreSQLOpenGaussTransaction(executionContext.getSqlStatementContext().getSqlStatement());
    }
    
    private boolean isExecuteDDLInXATransaction(final SQLStatement sqlStatement) {
        TransactionType transactionType = TransactionUtils.getTransactionType(databaseConnectionManager.getConnectionSession().getConnectionContext().getTransactionContext());
        TransactionStatus transactionStatus = databaseConnectionManager.getConnectionSession().getTransactionStatus();
        return TransactionType.XA == transactionType && transactionStatus.isInTransaction() && isUnsupportedDDLStatement(sqlStatement);
    }
    
    private boolean isExecuteDDLInPostgreSQLOpenGaussTransaction(final SQLStatement sqlStatement) {
        // TODO implement DDL statement commit/rollback in PostgreSQL/openGauss transaction
        boolean isPostgreSQLOpenGaussStatement = isPostgreSQLOrOpenGaussStatement(sqlStatement);
        boolean isSupportedStatement = isSupportedSQLStatement(sqlStatement);
        return sqlStatement instanceof DDLStatement
                && !isSupportedStatement && isPostgreSQLOpenGaussStatement && databaseConnectionManager.getConnectionSession().getTransactionStatus().isInTransaction();
    }
    
    private boolean isSupportedSQLStatement(final SQLStatement sqlStatement) {
        return isCursorStatement(sqlStatement) || sqlStatement instanceof TruncateStatement;
    }
    
    private boolean isCursorStatement(final SQLStatement sqlStatement) {
        return sqlStatement instanceof OpenGaussCursorStatement
                || sqlStatement instanceof CloseStatement || sqlStatement instanceof MoveStatement || sqlStatement instanceof FetchStatement;
    }
    
    private boolean isUnsupportedDDLStatement(final SQLStatement sqlStatement) {
        if (isPostgreSQLOrOpenGaussStatement(sqlStatement) && isSupportedSQLStatement(sqlStatement)) {
            return false;
        }
        return sqlStatement instanceof DDLStatement;
    }
    
    private boolean isPostgreSQLOrOpenGaussStatement(final SQLStatement sqlStatement) {
        return sqlStatement instanceof PostgreSQLStatement || sqlStatement instanceof OpenGaussStatement;
    }
    
    private String getTableName(final ExecutionContext executionContext) {
        return executionContext.getSqlStatementContext() instanceof TableAvailable && !((TableAvailable) executionContext.getSqlStatementContext()).getTablesContext().getSimpleTables().isEmpty()
                ? ((TableAvailable) executionContext.getSqlStatementContext()).getTablesContext().getSimpleTables().iterator().next().getTableName().getIdentifier().getValue()
                : "unknown_table";
    }
    
    /**
     * Execute SQL.
     *
     * @param executionContext execution context
     * @return execute results
     * @throws SQLException SQL exception
     */
    public List<ExecuteResult> execute(final ExecutionContext executionContext) throws SQLException {
        String databaseName = databaseConnectionManager.getConnectionSession().getDatabaseName();
        Collection<ShardingSphereRule> rules = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getDatabase(databaseName).getRuleMetaData().getRules();
        int maxConnectionsSizePerQuery = ProxyContext.getInstance()
                .getContextManager().getMetaDataContexts().getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY);
        boolean isReturnGeneratedKeys = executionContext.getSqlStatementContext().getSqlStatement() instanceof MySQLInsertStatement;
        return hasRawExecutionRule(rules) ? rawExecute(executionContext, rules, maxConnectionsSizePerQuery)
                : useDriverToExecute(executionContext, rules, maxConnectionsSizePerQuery, isReturnGeneratedKeys, SQLExecutorExceptionHandler.isExceptionThrown());
    }
    
    private boolean hasRawExecutionRule(final Collection<ShardingSphereRule> rules) {
        for (ShardingSphereRule each : rules) {
            if (each.getAttributes().findAttribute(RawExecutionRuleAttribute.class).isPresent()) {
                return true;
            }
        }
        return false;
    }
    
    private List<ExecuteResult> rawExecute(final ExecutionContext executionContext, final Collection<ShardingSphereRule> rules, final int maxConnectionsSizePerQuery) throws SQLException {
        RawExecutionPrepareEngine prepareEngine = new RawExecutionPrepareEngine(maxConnectionsSizePerQuery, rules);
        ExecutionGroupContext<RawSQLExecutionUnit> executionGroupContext;
        try {
            String databaseName = databaseConnectionManager.getConnectionSession().getDatabaseName();
            executionGroupContext = prepareEngine.prepare(databaseName, executionContext.getRouteContext(), executionContext.getExecutionUnits(),
                    new ExecutionGroupReportContext(databaseConnectionManager.getConnectionSession().getProcessId(),
                            databaseName, databaseConnectionManager.getConnectionSession().getConnectionContext().getGrantee()));
        } catch (final SQLException ex) {
            return getSaneExecuteResults(executionContext, ex);
        }
        // TODO handle query header
        return rawExecutor.execute(executionGroupContext, executionContext.getQueryContext(), new RawSQLExecutorCallback());
    }
    
    private List<ExecuteResult> useDriverToExecute(final ExecutionContext executionContext, final Collection<ShardingSphereRule> rules,
                                                   final int maxConnectionsSizePerQuery, final boolean isReturnGeneratedKeys, final boolean isExceptionThrown) throws SQLException {
        JDBCBackendStatement statementManager = (JDBCBackendStatement) databaseConnectionManager.getConnectionSession().getStatementManager();
        String databaseName = databaseConnectionManager.getConnectionSession().getDatabaseName();
        DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine = new DriverExecutionPrepareEngine<>(
                type, maxConnectionsSizePerQuery, databaseConnectionManager, statementManager, new StatementOption(isReturnGeneratedKeys), rules,
                ProxyContext.getInstance().getContextManager().getDatabase(databaseName).getResourceMetaData().getStorageUnits());
        ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext;
        try {
            executionGroupContext = prepareEngine.prepare(databaseName, executionContext.getRouteContext(), executionContext.getExecutionUnits(),
                    new ExecutionGroupReportContext(databaseConnectionManager.getConnectionSession().getProcessId(),
                            databaseName, databaseConnectionManager.getConnectionSession().getConnectionContext().getGrantee()));
        } catch (final SQLException ex) {
            return getSaneExecuteResults(executionContext, ex);
        }
        executeTransactionHooksBeforeExecuteSQL(databaseConnectionManager.getConnectionSession());
        return regularExecutor.execute(executionContext.getQueryContext(), executionGroupContext, isReturnGeneratedKeys, isExceptionThrown);
    }
    
    private void executeTransactionHooksBeforeExecuteSQL(final ConnectionSession connectionSession) throws SQLException {
        if (!getTransactionContext(connectionSession).isInTransaction()) {
            return;
        }
        for (TransactionHook each : transactionHooks) {
            each.beforeExecuteSQL(connectionSession.getDatabaseConnectionManager().getCachedConnections().values(), getTransactionContext(connectionSession), connectionSession.getIsolationLevel());
        }
    }
    
    private TransactionConnectionContext getTransactionContext(final ConnectionSession connectionSession) {
        return connectionSession.getDatabaseConnectionManager().getConnectionSession().getConnectionContext().getTransactionContext();
    }
    
    private List<ExecuteResult> getSaneExecuteResults(final ExecutionContext executionContext, final SQLException originalException) throws SQLException {
        DatabaseType databaseType = ProxyContext.getInstance().getContextManager().getDatabase(databaseConnectionManager.getConnectionSession().getDatabaseName()).getProtocolType();
        Optional<ExecuteResult> executeResult = new SaneQueryResultEngine(databaseType).getSaneQueryResult(executionContext.getSqlStatementContext().getSqlStatement(), originalException);
        return executeResult.map(Collections::singletonList).orElseThrow(() -> originalException);
    }
}
