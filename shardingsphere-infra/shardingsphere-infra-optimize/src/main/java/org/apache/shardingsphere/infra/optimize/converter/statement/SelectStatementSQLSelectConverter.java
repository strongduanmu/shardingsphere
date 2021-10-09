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

package org.apache.shardingsphere.infra.optimize.converter.statement;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.shardingsphere.infra.optimize.converter.segment.from.TableConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.groupby.GroupByConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.groupby.HavingConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.limit.OffsetConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.limit.RowCountConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.orderby.OrderByConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.projection.DistinctConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.projection.ProjectionsConverter;
import org.apache.shardingsphere.infra.optimize.converter.segment.where.WhereConverter;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ProjectionsSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.limit.LimitSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.handler.dml.SelectStatementHandler;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dml.MySQLSelectStatement;

import java.util.Optional;

/**
 * Select statement and SQL select converter.
 */
public final class SelectStatementSQLSelectConverter implements SQLStatementSQLNodeConverter<SelectStatement, SqlSelect> {
    
    @Override
    public SqlSelect convertSQLNode(final SelectStatement sqlStatement) {
        Optional<SqlNodeList> distinct = new DistinctConverter().convertSQLNode(sqlStatement.getProjections());
        Optional<SqlNodeList> projections = new ProjectionsConverter().convertSQLNode(sqlStatement.getProjections());
        Preconditions.checkState(projections.isPresent());
        Optional<SqlNode> from = new TableConverter().convertSQLNode(sqlStatement.getFrom());
        Optional<SqlNode> where = new WhereConverter().convertSQLNode(sqlStatement.getWhere().orElse(null));
        Optional<SqlNodeList> groupBy = new GroupByConverter().convertSQLNode(sqlStatement.getGroupBy().orElse(null));
        Optional<SqlNode> having = new HavingConverter().convertSQLNode(sqlStatement.getHaving().orElse(null));
        Optional<SqlNodeList> orderBy = new OrderByConverter().convertSQLNode(sqlStatement.getOrderBy().orElse(null));
        Optional<LimitSegment> limit = SelectStatementHandler.getLimitSegment(sqlStatement);
        Optional<SqlNode> offset = new OffsetConverter().convertSQLNode(limit.orElse(null));
        Optional<SqlNode> rowCount = new RowCountConverter().convertSQLNode(limit.orElse(null));
        return new SqlSelect(SqlParserPos.ZERO, distinct.orElse(null), projections.get(), from.orElse(null), where.orElse(null), groupBy.orElse(null),
                having.orElse(null), SqlNodeList.EMPTY, orderBy.orElse(null), offset.orElse(null), rowCount.orElse(null), SqlNodeList.EMPTY);
    }
    
    @Override
    public SelectStatement convertSQLStatement(final SqlSelect sqlNode) {
        Optional<ProjectionsSegment> projections = new DistinctConverter().convertSQLSegment(sqlNode.getSelectList());
        Preconditions.checkState(projections.isPresent());
        MySQLSelectStatement result = new MySQLSelectStatement();
        result.setProjections(projections.get());
        new TableConverter().convertSQLSegment(sqlNode.getFrom()).ifPresent(result::setFrom);
        new WhereConverter().convertSQLSegment(sqlNode.getWhere()).ifPresent(result::setWhere);
        new GroupByConverter().convertSQLSegment(sqlNode.getGroup()).ifPresent(result::setGroupBy);
        new HavingConverter().convertSQLSegment(sqlNode.getHaving()).ifPresent(result::setHaving);
        new OrderByConverter().convertSQLSegment(sqlNode.getOrderList()).ifPresent(result::setOrderBy);
        new OffsetConverter().convertSQLSegment(sqlNode.getOffset()).ifPresent(result::setLimit);
        new RowCountConverter().convertSQLSegment(sqlNode.getOffset()).ifPresent(result::setLimit);
        return result;
    }
}
