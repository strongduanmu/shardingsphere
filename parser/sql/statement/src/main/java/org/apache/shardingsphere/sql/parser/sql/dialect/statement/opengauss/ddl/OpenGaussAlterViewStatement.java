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

package org.apache.shardingsphere.sql.parser.sql.dialect.statement.opengauss.ddl;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.AlterViewStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.opengauss.OpenGaussStatement;

import java.util.Optional;

/**
 * OpenGauss alter view statement.
 */
@Getter
@Setter
public final class OpenGaussAlterViewStatement extends AlterViewStatement implements OpenGaussStatement {
    
    private SimpleTableSegment renameView;
    
    /**
     * Get rename view.
     *
     * @return rename view
     */
    public Optional<SimpleTableSegment> getRenameView() {
        return Optional.ofNullable(renameView);
    }
}