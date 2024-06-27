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

package org.apache.shardingsphere.infra.binder.context.segment.table;

import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.AliasSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TablesContextTest {
    
    @Test
    void assertGetTableNames() {
        TablesContext tablesContext = new TablesContext(Arrays.asList(createTableSegment("table_1", "tbl_1"),
                createTableSegment("table_2", "tbl_2")), TypedSPILoader.getService(DatabaseType.class, "FIXTURE"));
        assertThat(tablesContext.getTableNames(), is(new HashSet<>(Arrays.asList("table_1", "table_2"))));
    }
    
    @Test
    void assertInstanceCreatedWhenNoExceptionThrown() {
        SimpleTableSegment tableSegment = new SimpleTableSegment(new TableNameSegment(0, 10, new IdentifierValue("tbl")));
        tableSegment.setOwner(new OwnerSegment(0, 0, new IdentifierValue("schema")));
        TablesContext tablesContext = new TablesContext(Collections.singleton(tableSegment), TypedSPILoader.getService(DatabaseType.class, "FIXTURE"));
        assertThat(tablesContext.getDatabaseName(), is(Optional.of("schema")));
        assertThat(tablesContext.getSchemaName(), is(Optional.of("schema")));
        assertThat(tablesContext.getTableNames(), is(Collections.singleton("tbl")));
    }
    
    private SimpleTableSegment createTableSegment(final String tableName, final String alias) {
        SimpleTableSegment result = new SimpleTableSegment(new TableNameSegment(0, 0, new IdentifierValue(tableName)));
        AliasSegment aliasSegment = new AliasSegment(0, 0, new IdentifierValue(alias));
        result.setAlias(aliasSegment);
        return result;
    }
    
    @Test
    void assertGetSchemaNameWithSameSchemaAndSameTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_1", "tbl_1");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        TablesContext tablesContext = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), TypedSPILoader.getService(DatabaseType.class, "FIXTURE"));
        assertTrue(tablesContext.getDatabaseName().isPresent());
        assertThat(tablesContext.getDatabaseName().get(), is("sharding_db_1"));
    }
    
    @Test
    void assertGetSchemaNameWithSameSchemaAndDifferentTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        TablesContext tablesContext = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), TypedSPILoader.getService(DatabaseType.class, "FIXTURE"));
        assertTrue(tablesContext.getDatabaseName().isPresent());
        assertThat(tablesContext.getDatabaseName().get(), is("sharding_db_1"));
    }
    
    @Test
    void assertGetSchemaNameWithDifferentSchemaAndSameTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_1", "tbl_1");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_2")));
        assertThrows(IllegalStateException.class, () -> new TablesContext(Arrays.asList(tableSegment1, tableSegment2), TypedSPILoader.getService(DatabaseType.class, "FIXTURE")).getDatabaseName());
    }
    
    @Test
    void assertGetSchemaNameWithDifferentSchemaAndDifferentTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_2")));
        assertThrows(IllegalStateException.class, () -> new TablesContext(Arrays.asList(tableSegment1, tableSegment2), TypedSPILoader.getService(DatabaseType.class, "FIXTURE")).getDatabaseName());
    }
    
    @Test
    void assertGetSchemaName() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        TablesContext tablesContext = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), TypedSPILoader.getService(DatabaseType.class, "FIXTURE"));
        assertTrue(tablesContext.getSchemaName().isPresent());
        assertThat(tablesContext.getSchemaName().get(), is("sharding_db_1"));
    }
}
