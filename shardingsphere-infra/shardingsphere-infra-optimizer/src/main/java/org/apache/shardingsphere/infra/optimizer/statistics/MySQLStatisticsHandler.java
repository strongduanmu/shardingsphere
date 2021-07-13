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
import org.apache.shardingsphere.infra.metadata.resource.DataSourcesMetaData;
import org.apache.shardingsphere.infra.metadata.resource.ShardingSphereResource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * StatisticsHandler implementation for MySQL.
 */
public final class MySQLStatisticsHandler implements StatisticsHandler {

    private static final String TABLE_ROW_SELECT_PREFIX = "SELECT table_schema, table_name, table_rows FROM information_schema.tables WHERE ";

    @Override
    public long handleTableRowCount(final Map<String, Collection<String>> datasourceToTables, final ShardingSphereResource shardingSphereResource) {
        long rowCount = 0L;
        Map<Map.Entry<String, Integer>, Collection<DataSourceMetaData>> map = new HashMap<>();
        DataSourcesMetaData dataSourcesMetaData = shardingSphereResource.getDataSourcesMetaData();
        for (Map.Entry<String, Collection<String>> entry : datasourceToTables.entrySet()) {
            DataSourceMetaData dataSourceMetaData = dataSourcesMetaData.getDataSourceMetaData(entry.getKey());
            Map.Entry<String, Integer> instEntry = new AbstractMap.SimpleEntry<>(dataSourceMetaData.getHostName(), dataSourceMetaData.getPort());
            if (!map.containsKey(instEntry)) {
                map.put(instEntry, new ArrayList<>());
            }
            map.get(instEntry).add(dataSourceMetaData);
        }
        for (Collection<DataSourceMetaData> dataSourceMetaData : map.values()) {
            List<String> databaseNames = dataSourceMetaData.stream().map(DataSourceMetaData::getCatalog).collect(Collectors.toList());
            List<String> tableNames = databaseNames.stream().map(datasourceToTables::get).filter(Objects::nonNull)
                    .flatMap(Collection::stream).collect(Collectors.toList());
            DataSource dataSource = shardingSphereResource.getDataSources().get(databaseNames.get(0));
            rowCount += getRowCountByDbAndTable(databaseNames, tableNames, dataSource);
        }
        return rowCount;
    }

    private double getRowCountByDbAndTable(final List<String> databaseNames, final List<String> tableNames, 
                                           final DataSource dataSource) {
        StringBuilder dbNameFilter = new StringBuilder(" table_schema IN (");
        for (String databaseName : databaseNames) {
            dbNameFilter.append("'").append(databaseName).append("',");
        }
        dbNameFilter.deleteCharAt(dbNameFilter.length() - 1);
        dbNameFilter.append(") ");

        StringBuilder tableNameFilter = new StringBuilder(" table_name IN (");
        for (String tableName : tableNames) {
            tableNameFilter.append("'").append(tableName).append("',");
        }
        tableNameFilter.deleteCharAt(dbNameFilter.length() - 1);
        tableNameFilter.append(")");

        String sql = TABLE_ROW_SELECT_PREFIX + dbNameFilter + " AND " + tableNameFilter;
        return execute(dataSource, sql);
    }

    private double execute(final DataSource dataSource, final String sql) {
        double rowCount = 0d;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                rowCount += rs.getLong("TABLE_ROWS");
            }
        } catch (SQLException e) {
            // TODO
        }
        return rowCount;
    }
}
