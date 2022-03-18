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

package org.apache.shardingsphere.infra.metadata.schema;

import lombok.Getter;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ShardingSphere database.
 */
@Getter
public final class ShardingSphereDatabase {
    
    private final Map<String, ShardingSphereSchema> schemaMap;
    
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public ShardingSphereDatabase() {
        schemaMap = new ConcurrentHashMap<>();
    }
    
    public ShardingSphereDatabase(final Map<String, ShardingSphereSchema> schemaMap) {
        this.schemaMap = new ConcurrentHashMap<>(schemaMap.size(), 1);
        schemaMap.forEach((key, value) -> this.schemaMap.put(key.toLowerCase(), value));
    }
    
    /**
     * Get all table names.
     *
     * @return all table names
     */
    public Collection<String> getAllTableNames() {
        return schemaMap.keySet();
    }
    
    /**
     * Get table meta data via table name.
     * 
     * @param tableName tableName table name
     * @return table meta data
     */
    public ShardingSphereSchema get(final String tableName) {
        return schemaMap.get(tableName.toLowerCase());
    }
    
    /**
     * Add table meta data.
     * 
     * @param tableName table name
     * @param tableMetaData table meta data
     */
    public void put(final String tableName, final ShardingSphereSchema tableMetaData) {
        schemaMap.put(tableName.toLowerCase(), tableMetaData);
    }
    
    /**
     * Add table meta data map.
     *
     * @param tableMetaDataMap table meta data map
     */
    public void putAll(final Map<String, ShardingSphereSchema> tableMetaDataMap) {
        for (Entry<String, ShardingSphereSchema> entry : tableMetaDataMap.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Remove table meta data.
     *
     * @param tableName table name
     */
    public void remove(final String tableName) {
        schemaMap.remove(tableName.toLowerCase());
    }
    
    /**
     * Judge contains table from table meta data or not.
     *
     * @param tableName table name
     * @return contains table from table meta data or not
     */
    public boolean containsTable(final String tableName) {
        return schemaMap.containsKey(tableName.toLowerCase());
    }
}
