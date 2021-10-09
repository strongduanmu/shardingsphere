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

package org.apache.shardingsphere.infra.optimize.converter.segment.projection.impl;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.shardingsphere.infra.optimize.converter.segment.SQLSegmentSQLNodeConverter;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ExpressionProjectionSegment;

import java.util.Optional;

/**
 * Expression projection converter.
 */
public final class ExpressionProjectionConverter implements SQLSegmentSQLNodeConverter<ExpressionProjectionSegment, SqlNode> {
    
    @Override
    public Optional<SqlNode> convertSQLNode(final ExpressionProjectionSegment segment) {
        // TODO expression has not been parsed now.
        String expression = segment.getText();
        return Optional.of(SqlCharStringLiteral.createCharString(expression, SqlParserPos.ZERO));
    }
    
    @Override
    public Optional<ExpressionProjectionSegment> convertSQLSegment(final SqlNode sqlNode) {
        return Optional.empty();
    }
}
