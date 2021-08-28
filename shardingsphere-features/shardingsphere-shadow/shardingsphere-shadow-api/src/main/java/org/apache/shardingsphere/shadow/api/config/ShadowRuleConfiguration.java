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

package org.apache.shardingsphere.shadow.api.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.function.DistributedRuleConfiguration;
import org.apache.shardingsphere.infra.config.scope.SchemaRuleConfiguration;
import org.apache.shardingsphere.shadow.api.config.datasource.ShadowDataSourceConfiguration;
import org.apache.shardingsphere.shadow.api.config.table.ShadowTableConfiguration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shadow rule configuration.
 */
@Getter
@Setter
@RequiredArgsConstructor
public final class ShadowRuleConfiguration implements SchemaRuleConfiguration, DistributedRuleConfiguration {
    
    private final String column;
    
    private final List<String> sourceDataSourceNames;
    
    private final List<String> shadowDataSourceNames;
    
    private final boolean enable;
    
    private Map<String, ShadowDataSourceConfiguration> dataSources = new LinkedHashMap<>();
    
    private Map<String, ShadowTableConfiguration> tables = new LinkedHashMap<>();
    
    private Map<String, ShardingSphereAlgorithmConfiguration> shadowAlgorithms = new LinkedHashMap<>();
    
    public ShadowRuleConfiguration(final String column, final List<String> sourceDataSourceNames, final List<String> shadowDataSourceNames) {
        this(column, sourceDataSourceNames, shadowDataSourceNames, false);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(column), "Column is required.");
        Preconditions.checkArgument(!sourceDataSourceNames.isEmpty(), "SourceDataSourceNames is required.");
        Preconditions.checkArgument(!shadowDataSourceNames.isEmpty(), "ShadowDataSourceNames is required.");
        Preconditions.checkArgument(sourceDataSourceNames.size() == shadowDataSourceNames.size(), "SourceDataSourceNames and ShadowDataSourceNames size must same.");
    }
}
