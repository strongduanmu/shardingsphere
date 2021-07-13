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

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collection of statistics for all table of logical database.
 */
@Getter
public final class TablesStatistics {

    private Map<String, TableStatistics> tableStatistics;

    public TablesStatistics() {
        tableStatistics = new ConcurrentHashMap<>();
    }

    /**
     * Get a instance of the <code>TableStatistics</code>, if not exist create a new one.
     * @param tableName table name
     * @return TableStatistics object
     */
    public TableStatistics getOrCreate(final String tableName) {
        TableStatistics table = tableStatistics.get(tableName);
        if (table == null) {
            tableStatistics.put(tableName, new TableStatistics(tableName));
        }
        return tableStatistics.get(tableName);
    }

    /**
     * Get row count of a logical table.
     * @param tableName logical table name.
     * @return row count of the logical table, 0 if statistic not exist
     */
    public long getTableRowCount(final String tableName) {
        TableStatistics table = tableStatistics.get(tableName);
        if (table == null) {
            return 0L;
        }
        return table.getRowCount();
    }
}
