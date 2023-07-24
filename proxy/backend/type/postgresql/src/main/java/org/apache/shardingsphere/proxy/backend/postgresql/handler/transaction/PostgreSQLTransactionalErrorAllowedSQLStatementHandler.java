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

package org.apache.shardingsphere.proxy.backend.postgresql.handler.transaction;

import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.proxy.backend.handler.transaction.TransactionalErrorAllowedSQLStatementHandler;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.tcl.CommitStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.tcl.RollbackStatement;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Transactional error allowed SQL statement handler of PostgreSQL.
 */
public final class PostgreSQLTransactionalErrorAllowedSQLStatementHandler implements TransactionalErrorAllowedSQLStatementHandler {
    
    @Override
    public void judgeContinueToExecute(final SQLStatement statement) throws SQLException {
        ShardingSpherePreconditions.checkState(statement instanceof CommitStatement || statement instanceof RollbackStatement,
                () -> new SQLFeatureNotSupportedException("Current transaction is aborted, commands ignored until end of transaction block."));
    }
    
    @Override
    public String getDatabaseType() {
        return "PostgreSQL";
    }
}
