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

package org.apache.shardingsphere.infra.optimizer.statistics;

import org.apache.shardingsphere.infra.database.metadata.DataSourceMetaData;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.dialect.MySQLDatabaseType;
import org.apache.shardingsphere.infra.metadata.resource.ShardingSphereResource;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.apache.shardingsphere.sharding.rule.TableRule;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Statistics provider for cbo optimization. This will provide:
 * <ul>
 *     <li>row count of table</li>
 *     <li>column count</li>
 *     <li>histogram</li>
 *     <li>NDV</li>
 * </ul>
 */
public final class StatisticsProvider {

    private static Map<String, StatisticsProvider> statisticsProviders = new ConcurrentHashMap<>();

    private final ShardingSphereResource shardingSphereResource;

    private final ShardingRule shardingRule;

    private StatisticsHandler statisticsHandler;

    private TablesStatistics tablesStatistics;

    private StatisticsProvider(final ShardingSphereResource shardingSphereResource,
                               final ShardingRule shardingRule, final DatabaseType databaseType) {
        this.shardingSphereResource = shardingSphereResource;
        this.shardingRule = shardingRule;
        tablesStatistics = new TablesStatistics();
        if(databaseType instanceof MySQLDatabaseType) {
            // TODO use SPI
            statisticsHandler = new MySQLStatisticsHandler();
        }
    }

    public double getRowCount(String tableName) {
        return tablesStatistics.getTableRowCount(tableName);
    }

    public void analyzeTable(String table) {
        analyzeTable(shardingRule.getTableRule(table));
    }

    private void analyzeTable(TableRule tableRule) {
        Map<Map.Entry<String, Integer>, Collection<DataSourceMetaData>> map = new HashMap<>();
        long rowCount = statisticsHandler.handleTableRowCount(tableRule.getDatasourceToTablesMap(), shardingSphereResource);
        String logicalTableName = tableRule.getLogicTable();
        TableStatistics table = tablesStatistics.getOrCreate(logicalTableName);
        table.setRowCount(rowCount);
    }

    public static void addStatisticsProvider(final String schemaName, final ShardingSphereResource shardingSphereResource,
                                             final ShardingRule shardingRule, final DatabaseType databaseType) {
        if(statisticsProviders.containsKey(schemaName)) {
            return;
        }
        StatisticsProvider statisticsProvider = new StatisticsProvider(shardingSphereResource, shardingRule, databaseType);
        statisticsProviders.put(schemaName, statisticsProvider);
    }

    public static StatisticsProvider get(String schemaName) {
        return statisticsProviders.get(schemaName);
    }
}