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

package org.apache.shardingsphere.infra.executor.sql.federate.original.row;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.shardingsphere.infra.exception.ShardingSphereException;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;
import org.apache.shardingsphere.infra.merge.result.MergedResult;

import java.sql.SQLException;

/**
 * Filterable row enumerator.
 */
public final class FilterableRowEnumerator implements Enumerator<Object[]> {
    
    private MergedResult result;
    
    private QueryResultMetaData metaData;
    
    private Object[] currentRow;
    
    public FilterableRowEnumerator(final MergedResult queryResult, final QueryResultMetaData metaData) {
        this.result = queryResult;
        this.metaData = metaData;
    }
    
    @Override
    public Object[] current() {
        return currentRow;
    }
    
    @Override
    public boolean moveNext() {
        try {
            return moveNext0();
        } catch (final SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private boolean moveNext0() throws SQLException {
        if (result.next()) {
            setCurrentRow();
            return true;
        }
        return false;
    }
    
    private void setCurrentRow() throws SQLException {
        currentRow = new Object[metaData.getColumnCount()];
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            currentRow[i] = result.getValue(i + 1, Object.class);
        }
    }
    
    @Override
    public void reset() {
    }
    
    @Override
    public void close() {
        currentRow = null;
    }
}
