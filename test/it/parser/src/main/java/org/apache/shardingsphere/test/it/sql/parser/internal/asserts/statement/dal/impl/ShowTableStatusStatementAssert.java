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

package org.apache.shardingsphere.test.it.sql.parser.internal.asserts.statement.dal.impl;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dal.ShowTableStatusStatement;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.SQLCaseAssertContext;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.SQLSegmentAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.database.DatabaseAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.show.ShowFilterAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.statement.dal.ShowTableStatusStatementTestCase;

/**
 * Show table status statement assert.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ShowTableStatusStatementAssert {
    
    /**
     * Assert show table status statement is correct with expected parser result.
     * 
     * @param assertContext assert context
     * @param actual actual show table status statement
     * @param expected expected show table status statement test case
     */
    public static void assertIs(final SQLCaseAssertContext assertContext, final ShowTableStatusStatement actual, final ShowTableStatusStatementTestCase expected) {
        if (actual.getFromSchema().isPresent()) {
            DatabaseAssert.assertIs(assertContext, actual.getFromSchema().get().getSchema(), expected.getFromSchema().getSchema());
            SQLSegmentAssert.assertIs(assertContext, actual.getFromSchema().get(), expected.getFromSchema());
        }
        if (actual.getFilter().isPresent()) {
            ShowFilterAssert.assertIs(assertContext, actual.getFilter().get(), expected.getFilter());
        }
    }
}
