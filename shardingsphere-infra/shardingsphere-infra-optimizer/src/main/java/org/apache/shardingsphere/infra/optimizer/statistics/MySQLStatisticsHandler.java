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
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;

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
    
    private static final String TABLE_COLUMN_CARDINALITY_SELECT_PREFIX = "SELECT table_schema, table_name, column_name, cardinality FROM information_schema.statistics WHERE ";

    @Override
    public long handleTableRowCount(final Map<String, Collection<String>> datasourceToTables, final ShardingSphereResource shardingSphereResource) {
        long rowCount = 0L;
        Map<Map.Entry<String, Integer>, Collection<DataSourceMetaData>> instToDbs = parseInstToDbList(datasourceToTables, shardingSphereResource.getDataSourcesMetaData());
        for (Collection<DataSourceMetaData> dataSourceMetaData : instToDbs.values()) {
            List<String> databaseNames = dataSourceMetaData.stream().map(DataSourceMetaData::getCatalog).collect(Collectors.toList());
            List<String> tableNames = databaseNames.stream().map(datasourceToTables::get).filter(Objects::nonNull)
                    .flatMap(Collection::stream).collect(Collectors.toList());
            DataSource dataSource = shardingSphereResource.getDataSources().get(databaseNames.get(0));
            rowCount += getRowCountByDbAndTable(databaseNames, tableNames, dataSource);
        }
        return rowCount;
    }
    
    @Override
    public Map<String, Long> handleTableColumnCardinality(final Map<String, Collection<String>> datasourceToTables, final TableMetaData tableMetaData,
                                                          final ShardingSphereResource shardingSphereResource) {
        Map<Map.Entry<String, Integer>, Collection<DataSourceMetaData>> instToDbs = parseInstToDbList(datasourceToTables, shardingSphereResource.getDataSourcesMetaData());
        String columnNameFilter = buildSqlSegment(tableMetaData.getColumns().keySet(), " column_name");
        Map<String, Long> columnCardinality = new HashMap<>();
        for (Collection<DataSourceMetaData> dataSourceMetaData : instToDbs.values()) {
            List<String> databaseNames = dataSourceMetaData.stream().map(DataSourceMetaData::getCatalog).collect(Collectors.toList());
            List<String> tableNames = databaseNames.stream().map(datasourceToTables::get).filter(Objects::nonNull).flatMap(Collection::stream).collect(Collectors.toList());
            DataSource dataSource = shardingSphereResource.getDataSources().get(databaseNames.get(0));
            Map<String, Long> cardinality = getColumnCardinalityByDbAndTable(databaseNames, tableNames, columnNameFilter, dataSource);
            cardinality.entrySet().forEach(entry -> 
                    columnCardinality.put(entry.getKey(), columnCardinality.getOrDefault(entry.getKey(), 0L) + entry.getValue()));
        }
        return columnCardinality;
    }
    
    private Map<String, Long> getColumnCardinalityByDbAndTable(final List<String> databaseNames, final List<String> tableNames, 
                                                               final String columnNameFilter, final DataSource dataSource) {
        String dbNameFilter = buildSqlSegment(databaseNames, " table_schema");
        String tableNameFilter = buildSqlSegment(tableNames, " table_name");
        String sql = TABLE_COLUMN_CARDINALITY_SELECT_PREFIX + dbNameFilter + " AND " + tableNameFilter + " AND " + columnNameFilter;
        return queryColumnCardinality(dataSource, sql);
    }
    
    private Map<Map.Entry<String, Integer>, Collection<DataSourceMetaData>> parseInstToDbList(final Map<String, Collection<String>> datasourceToTables, final DataSourcesMetaData dataSourcesMetaData) {
        Map<Map.Entry<String, Integer>, Collection<DataSourceMetaData>> map = new HashMap<>();
        for (Map.Entry<String, Collection<String>> entry : datasourceToTables.entrySet()) {
            DataSourceMetaData dataSourceMetaData = dataSourcesMetaData.getDataSourceMetaData(entry.getKey());
            Map.Entry<String, Integer> instEntry = new AbstractMap.SimpleEntry<>(dataSourceMetaData.getHostName(), dataSourceMetaData.getPort());
            if (!map.containsKey(instEntry)) {
                map.put(instEntry, new ArrayList<>());
            }
            map.get(instEntry).add(dataSourceMetaData);
        }
        return map;
    }

    private double getRowCountByDbAndTable(final List<String> databaseNames, final List<String> tableNames, 
                                           final DataSource dataSource) {
        String dbNameFilter = buildSqlSegment(databaseNames, " table_schema");
        String tableNameFilter = buildSqlSegment(tableNames, " table_name");
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

    private Map<String, Long> queryColumnCardinality(final DataSource dataSource, final String sql) {
        Map<String, Long> cardinality = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                cardinality.put(rs.getString("column_name"), rs.getLong("cardinality"));
            }
        } catch (SQLException e) {
            // TODO
        }
        return cardinality;
    }
    
    private String buildSqlSegment(final Collection<String> vals, final String identifier) {
        StringBuilder segment = new StringBuilder(identifier);
        segment.append(" IN (");
        for (String databaseName : vals) {
            segment.append("'").append(databaseName).append("',");
        }
        segment.deleteCharAt(segment.length() - 1);
        segment.append(") ");
        return segment.toString();
    }
}
