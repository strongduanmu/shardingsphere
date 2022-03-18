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

package org.apache.shardingsphere.infra.federation.executor.original;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.shardingsphere.infra.federation.executor.original.table.FilterableTable;
import org.apache.shardingsphere.infra.federation.executor.original.table.FilterableTableScanExecutor;
import org.apache.shardingsphere.infra.federation.optimizer.metadata.FederationSchemaMetaData;
import org.apache.shardingsphere.infra.federation.optimizer.metadata.FederationTableMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filterable schema.
 */
@Getter
public final class FilterableSchema extends AbstractSchema {
    
    private final String name;
    
//    private final Map<String, Table> tableMap;
    
    private final Map<String, Schema> subSchemaMap;
    
    public FilterableSchema(final FederationSchemaMetaData schemaMetaData, final FilterableTableScanExecutor executor) {
        name = schemaMetaData.getName();
//        tableMap = createTableMap(schemaMetaData, executor);
        subSchemaMap = createSubSchemaMap(executor);
    }
    
    private Map<String, Schema> createSubSchemaMap(final FilterableTableScanExecutor executor) {
        Map<String, Schema> result = new LinkedHashMap<>();
        Map<String, TableMetaData> metaData = new LinkedHashMap<>();
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("name", Types.VARCHAR, false, false, false));
        metaData.put("tables", new TableMetaData("tables", columnMetaDataList, Collections.emptyList(), Collections.emptyList()));
        result.put("information_schema", new FilterableSubSchema(new FederationSchemaMetaData("information_schema", metaData), executor));
        return result;
    }
    
    private Map<String, Table> createTableMap(final FederationSchemaMetaData schemaMetaData, final FilterableTableScanExecutor executor) {
        Map<String, Table> result = new LinkedHashMap<>(schemaMetaData.getTables().size(), 1);
        for (FederationTableMetaData each : schemaMetaData.getTables().values()) {
            result.put(each.getName(), new FilterableTable(each, executor, new FederationTableStatistic()));
        }
        return result;
    }
}
