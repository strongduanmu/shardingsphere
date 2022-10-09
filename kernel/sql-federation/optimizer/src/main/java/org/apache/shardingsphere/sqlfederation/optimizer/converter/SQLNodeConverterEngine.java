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

package org.apache.shardingsphere.sqlfederation.optimizer.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.combine.CombineSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sqlfederation.optimizer.converter.exception.OptimizationSQLNodeConvertException;
import org.apache.shardingsphere.sqlfederation.optimizer.converter.statement.select.SelectStatementConverter;
import org.apache.shardingsphere.sqlfederation.optimizer.converter.type.CombineOperatorConverter;

import java.util.Arrays;

/**
 * SQL node converter engine.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SQLNodeConverterEngine {
    
    /**
     * Convert SQL statement to SQL node.
     * 
     * @param statement SQL statement to be converted
     * @return sqlNode converted SQL node
     */
    public static SqlNode convert(final SQLStatement statement) {
        if (statement instanceof SelectStatement) {
            SqlNode result = new SelectStatementConverter().convert((SelectStatement) statement);
            if (((SelectStatement) statement).getCombine().isPresent()) {
                return convert(result, ((SelectStatement) statement));
            }
            return result;
        }
        throw new OptimizationSQLNodeConvertException(statement);
    }
    
    private static SqlNode convert(final SqlNode sqlNode, final SelectStatement selectStatement) {
        if (selectStatement.getCombine().isPresent()) {
            CombineSegment combineSegment = selectStatement.getCombine().get();
            SqlNode combineSqlNode = new SqlBasicCall(CombineOperatorConverter.convert(combineSegment.getCombineType()),
                    Arrays.asList(sqlNode, new SelectStatementConverter().convert(combineSegment.getSelectStatement())), SqlParserPos.ZERO);
            return convert(combineSqlNode, combineSegment.getSelectStatement());
        }
        return sqlNode;
    }
}
