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

package org.apache.shardingsphere.test.it.data.pipeline.core.fixture;

import org.apache.shardingsphere.data.pipeline.core.sqlbuilder.AbstractPipelineSQLBuilder;

import java.util.Optional;

public final class H2PipelineSQLBuilder extends AbstractPipelineSQLBuilder {
    
    @Override
    protected boolean isKeyword(final String item) {
        return false;
    }
    
    @Override
    protected String getLeftIdentifierQuoteString() {
        return "";
    }
    
    @Override
    protected String getRightIdentifierQuoteString() {
        return "";
    }
    
    @Override
    public Optional<String> buildEstimatedCountSQL(final String schemaName, final String tableName) {
        return Optional.empty();
    }
    
    @Override
    public String getType() {
        return "H2";
    }
}
