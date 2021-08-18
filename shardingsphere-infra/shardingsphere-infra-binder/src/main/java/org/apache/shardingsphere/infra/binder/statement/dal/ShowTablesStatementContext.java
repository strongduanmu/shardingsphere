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

package org.apache.shardingsphere.infra.binder.statement.dal;

import lombok.Getter;
import org.apache.shardingsphere.infra.binder.segment.table.TablesContext;
import org.apache.shardingsphere.infra.binder.statement.CommonSQLStatementContext;
import org.apache.shardingsphere.infra.binder.type.RemoveAvailable;
import org.apache.shardingsphere.infra.binder.type.TableAvailable;
import org.apache.shardingsphere.sql.parser.sql.common.segment.SQLSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dal.PatternSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.sql.common.value.identifier.IdentifierValue;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dal.MySQLShowTablesStatement;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Show tables statement context.
 */
@Getter
public final class ShowTablesStatementContext extends CommonSQLStatementContext<MySQLShowTablesStatement> implements RemoveAvailable, TableAvailable {
    
    private final TablesContext tablesContext;
    
    public ShowTablesStatementContext(final MySQLShowTablesStatement sqlStatement) {
        super(sqlStatement);
        tablesContext = new TablesContext(getAllSimpleTableSegments());
    }
    
    private Collection<SimpleTableSegment> getAllSimpleTableSegments() {
        Collection<SimpleTableSegment> result = new LinkedList<>();
        if (getSqlStatement().getLike().isPresent()) {
            PatternSegment pattern = getSqlStatement().getLike().get().getPattern();
            result.add(new SimpleTableSegment(new TableNameSegment(pattern.getStartIndex(), pattern.getStopIndex(), new IdentifierValue(pattern.getPattern()))));
        }
        // TODO extract where condition value into result
        return result;
    }
    
    @Override
    public Collection<SQLSegment> getRemoveSegments() {
        Collection<SQLSegment> result = new LinkedList<>();
        getSqlStatement().getFromSchema().ifPresent(result::add);
        return result;
    }
    
    @Override
    public Collection<SimpleTableSegment> getAllTables() {
        return tablesContext.getOriginalTables();
    }
    
    /**
     * Judge whether contains pattern matching or not.
     * 
     * @return whether contains pattern matching or not
     */
    public boolean containsPatternMatching() {
        return tablesContext.getTableNames().stream().anyMatch(each -> each.startsWith("%") || each.endsWith("%") || each.startsWith("_") || each.endsWith("_"));
    }
}
