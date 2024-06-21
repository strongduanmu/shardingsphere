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

package org.apache.shardingsphere.test.it.sql.parser.internal.asserts.statement.dcl.impl;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dcl.RevokeStatement;
import org.apache.shardingsphere.sql.parser.statement.mysql.dcl.MySQLRevokeStatement;
import org.apache.shardingsphere.sql.parser.statement.sqlserver.dcl.SQLServerRevokeStatement;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.SQLCaseAssertContext;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.statement.dcl.impl.mysql.MySQLRevokeStatementAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.statement.dcl.impl.sqlserver.SQLServerRevokeStatementAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.statement.dcl.RevokeStatementTestCase;

/**
 * Revoke statement assert.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RevokeStatementAssert {
    
    /**
     * Assert revoke statement is correct with expected parser result.
     * 
     * @param assertContext assert context
     * @param actual actual revoke statement
     * @param expected expected revoke statement test case
     */
    public static void assertIs(final SQLCaseAssertContext assertContext, final RevokeStatement actual, final RevokeStatementTestCase expected) {
        if (actual instanceof MySQLRevokeStatement) {
            MySQLRevokeStatementAssert.assertIs(assertContext, (MySQLRevokeStatement) actual, expected);
        } else if (actual instanceof SQLServerRevokeStatement) {
            SQLServerRevokeStatementAssert.assertIs(assertContext, (SQLServerRevokeStatement) actual, expected);
        }
    }
}
