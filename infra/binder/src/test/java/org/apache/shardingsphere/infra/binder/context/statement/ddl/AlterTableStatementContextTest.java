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

package org.apache.shardingsphere.infra.binder.context.statement.ddl;

import org.apache.shardingsphere.infra.binder.context.statement.CommonSQLStatementContext;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.alter.AddColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.alter.ModifyColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.constraint.ConstraintDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.constraint.ConstraintSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.constraint.alter.AddConstraintDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.constraint.alter.DropConstraintDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.constraint.alter.ValidateConstraintDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.index.DropIndexDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.index.IndexNameSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.index.IndexSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.AlterTableStatement;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;
import org.apache.shardingsphere.sql.parser.statement.mysql.ddl.MySQLAlterTableStatement;
import org.apache.shardingsphere.sql.parser.statement.oracle.ddl.OracleAlterTableStatement;
import org.apache.shardingsphere.sql.parser.statement.postgresql.ddl.PostgreSQLAlterTableStatement;
import org.apache.shardingsphere.sql.parser.statement.sql92.ddl.SQL92AlterTableStatement;
import org.apache.shardingsphere.sql.parser.statement.sqlserver.ddl.SQLServerAlterTableStatement;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlterTableStatementContextTest {
    
    @Test
    void assertMySQLNewInstance() {
        assertNewInstance(mock(MySQLAlterTableStatement.class));
    }
    
    @Test
    void assertPostgreSQLNewInstance() {
        assertNewInstance(mock(PostgreSQLAlterTableStatement.class));
    }
    
    @Test
    void assertOracleNewInstance() {
        assertNewInstance(mock(OracleAlterTableStatement.class));
    }
    
    @Test
    void assertSQLServerNewInstance() {
        assertNewInstance(mock(SQLServerAlterTableStatement.class));
    }
    
    @Test
    void assertSQL92NewInstance() {
        assertNewInstance(mock(SQL92AlterTableStatement.class));
    }
    
    private void assertNewInstance(final AlterTableStatement alterTableStatement) {
        SimpleTableSegment table = new SimpleTableSegment(new TableNameSegment(0, 0, new IdentifierValue("tbl_1")));
        SimpleTableSegment renameTable = new SimpleTableSegment(new TableNameSegment(0, 0, new IdentifierValue("rename_tbl_1")));
        when(alterTableStatement.getTable()).thenReturn(table);
        when(alterTableStatement.getRenameTable()).thenReturn(Optional.of(renameTable));
        Collection<SimpleTableSegment> referencedTables = Collections.singletonList(table);
        ColumnDefinitionSegment columnDefinition = mock(ColumnDefinitionSegment.class);
        when(columnDefinition.getReferencedTables()).thenReturn(referencedTables);
        AddColumnDefinitionSegment addColumnDefinition = mock(AddColumnDefinitionSegment.class);
        when(addColumnDefinition.getColumnDefinitions()).thenReturn(Collections.singletonList(columnDefinition));
        when(alterTableStatement.getAddColumnDefinitions()).thenReturn(Collections.singletonList(addColumnDefinition));
        ModifyColumnDefinitionSegment modifyColumnDefinition = mock(ModifyColumnDefinitionSegment.class);
        when(modifyColumnDefinition.getColumnDefinition()).thenReturn(columnDefinition);
        when(alterTableStatement.getModifyColumnDefinitions()).thenReturn(Collections.singletonList(modifyColumnDefinition));
        ConstraintDefinitionSegment constraintDefinition = mock(ConstraintDefinitionSegment.class);
        when(constraintDefinition.getReferencedTable()).thenReturn(Optional.of(table));
        when(constraintDefinition.getIndexName()).thenReturn(Optional.of(new IndexSegment(0, 0, new IndexNameSegment(0, 0, new IdentifierValue("index")))));
        AddConstraintDefinitionSegment addConstraintDefinition = mock(AddConstraintDefinitionSegment.class);
        when(addConstraintDefinition.getConstraintDefinition()).thenReturn(constraintDefinition);
        ConstraintSegment constraint = new ConstraintSegment(0, 0, new IdentifierValue("constraint"));
        when(addConstraintDefinition.getConstraintDefinition().getConstraintName()).thenReturn(Optional.of(constraint));
        when(alterTableStatement.getAddConstraintDefinitions()).thenReturn(Collections.singletonList(addConstraintDefinition));
        ValidateConstraintDefinitionSegment validateConstraintDefinition = mock(ValidateConstraintDefinitionSegment.class);
        when(validateConstraintDefinition.getConstraintName()).thenReturn(constraint);
        when(alterTableStatement.getValidateConstraintDefinitions()).thenReturn(Collections.singletonList(validateConstraintDefinition));
        DropConstraintDefinitionSegment dropConstraintDefinition = mock(DropConstraintDefinitionSegment.class);
        when(dropConstraintDefinition.getConstraintName()).thenReturn(constraint);
        when(alterTableStatement.getDropConstraintDefinitions()).thenReturn(Collections.singletonList(dropConstraintDefinition));
        DropIndexDefinitionSegment dropIndexDefinitionSegment = mock(DropIndexDefinitionSegment.class);
        when(dropIndexDefinitionSegment.getIndexSegment()).thenReturn(new IndexSegment(0, 0, new IndexNameSegment(0, 0, new IdentifierValue("drop_index"))));
        when(alterTableStatement.getDropIndexDefinitions()).thenReturn(Collections.singletonList(dropIndexDefinitionSegment));
        AlterTableStatementContext actual = new AlterTableStatementContext(alterTableStatement);
        assertThat(actual, instanceOf(CommonSQLStatementContext.class));
        assertThat(actual.getSqlStatement(), is(alterTableStatement));
        assertThat(actual.getTablesContext().getSimpleTables().stream().map(each -> each.getTableName().getIdentifier().getValue()).collect(Collectors.toList()),
                is(Arrays.asList("tbl_1", "rename_tbl_1", "tbl_1", "tbl_1", "tbl_1")));
        assertThat(actual.getIndexes().stream().map(each -> each.getIndexName().getIdentifier().getValue()).collect(Collectors.toList()), is(Arrays.asList("index", "drop_index")));
        assertThat(actual.getConstraints().stream().map(each -> each.getIdentifier().getValue()).collect(Collectors.toList()),
                is(Arrays.asList("constraint", "constraint", "constraint")));
    }
}
