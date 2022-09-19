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

package org.apache.shardingsphere.db.protocol.mysql.packet.binlog.management;

import io.netty.buffer.Unpooled;
import org.apache.shardingsphere.db.protocol.mysql.packet.binlog.MySQLBinlogEventHeader;
import org.apache.shardingsphere.db.protocol.mysql.payload.MySQLPacketPayload;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class MySQLBinlogRotateEventPacketTest {
    
    @Mock
    private MySQLPacketPayload payload;
    
    @Mock
    private MySQLBinlogEventHeader binlogEventHeader;
    
    @Test
    public void assertNew() {
        when(payload.readInt8()).thenReturn(4L);
        when(payload.readStringFix(anyInt())).thenReturn("binlog-000001");
        when(payload.getByteBuf()).thenReturn(Unpooled.buffer());
        MySQLBinlogRotateEventPacket actual = new MySQLBinlogRotateEventPacket(binlogEventHeader, payload);
        assertThat(actual.getSequenceId(), is(0));
        assertThat(actual.getBinlogEventHeader(), is(binlogEventHeader));
        assertThat(actual.getPosition(), is(4L));
        assertThat(actual.getNextBinlogName(), is("binlog-000001"));
    }
    
    @Test
    public void assertWrite() {
        new MySQLBinlogRotateEventPacket(binlogEventHeader, 4L, "binlog-000001").write(payload);
        verify(binlogEventHeader).write(payload);
        verify(payload).writeInt8(4L);
        verify(payload).writeStringEOF("binlog-000001");
    }
}
