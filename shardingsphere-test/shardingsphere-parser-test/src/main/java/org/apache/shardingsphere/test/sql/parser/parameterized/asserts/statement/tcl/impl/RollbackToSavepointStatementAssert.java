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

package org.apache.shardingsphere.test.sql.parser.parameterized.asserts.statement.tcl.impl;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.sql.parser.sql.common.statement.tcl.RollbackToSavepointStatement;
import org.apache.shardingsphere.test.sql.parser.parameterized.asserts.SQLCaseAssertContext;
import org.apache.shardingsphere.test.sql.parser.parameterized.jaxb.cases.domain.statement.tcl.RollbackStatementTestCase;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Rollback statement assert.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RollbackToSavepointStatementAssert {
    
    /**
     * Assert rollback to savepoint statement is correct with expected parser result.
     * 
     * @param assertContext assert context
     * @param actual actual rollback to savepoint statement
     * @param expected expected rollback to savepoint statement test case
     */
    public static void assertIs(final SQLCaseAssertContext assertContext, final RollbackToSavepointStatement actual, final RollbackStatementTestCase expected) {
        Optional<String> savepointName = Optional.ofNullable(actual.getSavepointName());
        if (null != expected.getSavepointName()) {
            assertTrue(assertContext.getText("Actual savepoint name should exist."), savepointName.isPresent());
            assertThat(assertContext.getText("Savepoint name assertion error."), savepointName.get(), is(expected.getSavepointName()));
        } else {
            assertFalse(assertContext.getText("Actual savepoint name should not exist."), savepointName.isPresent());
        }
    }
}
