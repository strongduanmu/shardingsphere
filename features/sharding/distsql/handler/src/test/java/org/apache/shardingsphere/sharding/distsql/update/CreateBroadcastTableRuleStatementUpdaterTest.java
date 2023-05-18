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

package org.apache.shardingsphere.sharding.distsql.update;

import org.apache.shardingsphere.distsql.handler.exception.rule.DuplicateRuleException;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.distsql.handler.update.CreateBroadcastTableRuleStatementUpdater;
import org.apache.shardingsphere.sharding.distsql.parser.statement.CreateBroadcastTableRuleStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class CreateBroadcastTableRuleStatementUpdaterTest {
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ShardingSphereDatabase database;
    
    private final CreateBroadcastTableRuleStatementUpdater updater = new CreateBroadcastTableRuleStatementUpdater();
    
    @Test
    void assertCreateBroadcastRule() {
        ShardingRuleConfiguration currentRuleConfig = new ShardingRuleConfiguration();
        CreateBroadcastTableRuleStatement sqlStatement = new CreateBroadcastTableRuleStatement(false, Arrays.asList("t_1", "t_2"));
        updater.checkSQLStatement(database, sqlStatement, currentRuleConfig);
        ShardingRuleConfiguration toBeCreatedRuleConfig = updater.buildToBeCreatedRuleConfiguration(currentRuleConfig, sqlStatement);
        updater.updateCurrentRuleConfiguration(currentRuleConfig, toBeCreatedRuleConfig);
        assertThat(currentRuleConfig.getBroadcastTables().size(), is(2));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_1"));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_2"));
    }
    
    @Test
    void assertCheckDuplicatedBroadcastTable() {
        ShardingRuleConfiguration currentRuleConfig = createCurrentRuleConfiguration();
        CreateBroadcastTableRuleStatement sqlStatement = new CreateBroadcastTableRuleStatement(false, Arrays.asList("t_order", "t_address"));
        assertThrows(DuplicateRuleException.class, () -> updater.checkSQLStatement(database, sqlStatement, currentRuleConfig));
    }
    
    @Test
    void assertCreateBroadcastRuleWhenBroadcastRuleExistsInCurrentRuleConfig() {
        ShardingRuleConfiguration currentRuleConfig = createCurrentRuleConfiguration();
        CreateBroadcastTableRuleStatement sqlStatement = new CreateBroadcastTableRuleStatement(false, Arrays.asList("t_1", "t_2"));
        updater.checkSQLStatement(database, sqlStatement, currentRuleConfig);
        ShardingRuleConfiguration toBeCreatedRuleConfig = updater.buildToBeCreatedRuleConfiguration(currentRuleConfig, sqlStatement);
        updater.updateCurrentRuleConfiguration(currentRuleConfig, toBeCreatedRuleConfig);
        assertThat(currentRuleConfig.getBroadcastTables().size(), is(4));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_1"));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_2"));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_order"));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_address"));
    }
    
    @Test
    void assertCreateBroadcastRuleWithIfNotExist() {
        Collection<String> tables = new LinkedList<>();
        tables.add("t_1");
        tables.add("t_2");
        tables.add("t_order");
        tables.add("t_address");
        ShardingRuleConfiguration currentRuleConfig = createCurrentRuleConfiguration();
        CreateBroadcastTableRuleStatement sqlStatement = new CreateBroadcastTableRuleStatement(true, tables);
        updater.checkSQLStatement(database, sqlStatement, currentRuleConfig);
        ShardingRuleConfiguration toBeCreatedRuleConfig = updater.buildToBeCreatedRuleConfiguration(currentRuleConfig, sqlStatement);
        updater.updateCurrentRuleConfiguration(currentRuleConfig, toBeCreatedRuleConfig);
        assertThat(currentRuleConfig.getBroadcastTables().size(), is(4));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_1"));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_2"));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_order"));
        assertTrue(currentRuleConfig.getBroadcastTables().contains("t_address"));
    }
    
    private ShardingRuleConfiguration createCurrentRuleConfiguration() {
        ShardingRuleConfiguration result = new ShardingRuleConfiguration();
        result.getBroadcastTables().addAll(Arrays.asList("t_order", "t_address"));
        return result;
    }
}
