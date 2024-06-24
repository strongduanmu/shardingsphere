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

package org.apache.shardingsphere.proxy.backend.state.impl;

import org.apache.shardingsphere.distsql.statement.ral.updatable.UpdatableRALStatement;
import org.apache.shardingsphere.distsql.statement.ral.updatable.UnlockClusterStatement;
import org.apache.shardingsphere.distsql.statement.rdl.RDLStatement;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.mode.exception.ClusterStateException;
import org.apache.shardingsphere.proxy.backend.state.ProxyClusterState;
import org.apache.shardingsphere.proxy.backend.state.SQLSupportedJudgeEngine;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.DDLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.UpdateStatement;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * ReadOnly proxy state.
 */
public final class ReadOnlyProxyState implements ProxyClusterState {
    
    private static final Collection<Class<? extends SQLStatement>> SUPPORTED_SQL_STATEMENTS = Collections.singleton(UnlockClusterStatement.class);
    
    private static final Collection<Class<? extends SQLStatement>> UNSUPPORTED_SQL_STATEMENTS = Arrays.asList(
            InsertStatement.class, UpdateStatement.class, DeleteStatement.class, DDLStatement.class, UpdatableRALStatement.class, RDLStatement.class);
    
    private final SQLSupportedJudgeEngine judgeEngine = new SQLSupportedJudgeEngine(SUPPORTED_SQL_STATEMENTS, UNSUPPORTED_SQL_STATEMENTS);
    
    @Override
    public void check(final SQLStatement sqlStatement) {
        ShardingSpherePreconditions.checkState(judgeEngine.isSupported(sqlStatement), () -> new ClusterStateException(getType(), sqlStatement));
    }
    
    @Override
    public String getType() {
        return "READ_ONLY";
    }
}
