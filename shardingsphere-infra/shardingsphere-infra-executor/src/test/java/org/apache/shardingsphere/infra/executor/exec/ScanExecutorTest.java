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

package org.apache.shardingsphere.infra.executor.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.shardingsphere.infra.executor.exec.meta.Row;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;
import org.apache.shardingsphere.infra.optimizer.rel.physical.SSScan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ScanExecutorTest extends BaseExecutorTest {
    
    @Before
    public void init() {
        relBuilder = relBuilder();
    }
    
    @Test
    public void testScanTable() {
        relBuilder.scan("t_order");
        List<RexNode> predicates = new ArrayList<>();
        predicates.add(relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.field("order_id"), relBuilder.literal(10000)));
        predicates.add(relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.field("user_id"), relBuilder.literal(10)));
        relBuilder.filter(predicates);
        RelNode relNode = relBuilder.build();
        SSScan scan = SSScan.create(relBuilder.getCluster(), relNode.getTraitSet(), relNode);
        Executor executor = buildExecutor(scan);
        QueryResultMetaData metaData = executor.getMetaData();
        Assert.assertNotNull(metaData);
        int rowCount = 0;
        while (executor.moveNext()) {
            Row row = executor.current();
            print(row);
            Assert.assertNotNull(row);
            rowCount++;
        }
        Assert.assertEquals(1, rowCount);
        executor.close();
    }
    
    @Test
    public void testTableScanWithProject() throws SQLException {
        // single table single row with partial column
        relBuilder.scan("t_order");
        List<RexNode> predicates = new ArrayList<>();
        predicates.add(relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.field("order_id"), relBuilder.literal(10000)));
        predicates.add(relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.field("user_id"), relBuilder.literal(10)));
        relBuilder.filter(predicates);
        relBuilder.project(relBuilder.field("status"), relBuilder.field("user_id"));
        RelNode relNode = relBuilder.build();
        SSScan scan = new SSScan(relBuilder.getCluster(), relNode.getTraitSet(), relNode);
        Executor executor = buildExecutor(scan);
        Assert.assertEquals(2, executor.getMetaData().getColumnCount());
        Map<String, Integer> columnNameIdxMap = createColumnLabelAndIndexMap(executor.getMetaData());
        int rowCount = 0;
        while (executor.moveNext()) {
            Row row = executor.current();
            print(row);
            Assert.assertNotNull(row);
            int userId = row.getColumnValue(columnNameIdxMap.get("user_id"));
            Assert.assertEquals(10, userId);
            String status = row.getColumnValue(columnNameIdxMap.get("status"));
            Assert.assertEquals("init", status);
            rowCount++;
        }
        Assert.assertEquals(1, rowCount);
        executor.close();
    }
    
    /**
     * TODO The testcase of join that been pushed down has not passed.
     */
    @Test
    public void testJoin() {
        // join table single row with full column
        relBuilder.scan("t_order");
        List<RexNode> predicates = new ArrayList<>();
        predicates.add(relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.field("order_id"), relBuilder.literal(1000)));
        predicates.add(relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.field("user_id"), relBuilder.literal(11)));
        relBuilder.filter(predicates).scan("t_order_item").filter(predicates)
                .join(JoinRelType.INNER, "user_id");
        relBuilder.filter(predicates);
        RelNode relNode = relBuilder.build();
        SSScan scan = new SSScan(relBuilder.getCluster(), relNode.getTraitSet(), relNode);
        Executor executor = buildExecutor(scan);
        while (executor.moveNext()) {
            Row row = executor.current();
            print(row);
            Assert.assertNotNull(row);
        }
        executor.close();
    }
    
    @Test
    public void testJoinWithSort() {
        // merge sort
        
    }
}
