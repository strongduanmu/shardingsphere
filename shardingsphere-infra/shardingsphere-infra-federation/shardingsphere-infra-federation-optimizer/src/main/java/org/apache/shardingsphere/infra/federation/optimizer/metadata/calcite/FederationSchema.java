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

package org.apache.shardingsphere.infra.federation.optimizer.metadata.calcite;

import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
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
 * Federation schema.
 */
@Getter
public final class FederationSchema extends AbstractSchema {
    
    private final Map<String, Schema> subSchemaMap;
    
    public FederationSchema(final FederationSchemaMetaData metaData) {
        subSchemaMap = createSubSchemaMap();
    }
    
    private Map<String, Table> getTableMap(final FederationSchemaMetaData metaData) {
        Map<String, Table> result = new LinkedHashMap<>(metaData.getTables().size(), 1);
        for (FederationTableMetaData each : metaData.getTables().values()) {
            result.put(each.getName(), new FederationTable(each));
        }
        return result;
    }
    
    private Map<String, Schema> createSubSchemaMap() {
        Map<String, Schema> result = new LinkedHashMap<>();
        Map<String, TableMetaData> metaData = new LinkedHashMap<>();
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("name", Types.VARCHAR, false, false, false));
        metaData.put("tables", new TableMetaData("tables", columnMetaDataList, Collections.emptyList(), Collections.emptyList()));
        result.put("information_schema", new FederationSubSchema(new FederationSchemaMetaData("information_schema", metaData)));
        return result;
    }
}
