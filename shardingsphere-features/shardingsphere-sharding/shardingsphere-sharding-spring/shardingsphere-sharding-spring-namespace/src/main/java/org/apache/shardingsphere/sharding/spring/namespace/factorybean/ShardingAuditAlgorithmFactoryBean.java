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

package org.apache.shardingsphere.sharding.spring.namespace.factorybean;

import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.sharding.factory.ShardingAuditAlgorithmFactory;
import org.apache.shardingsphere.sharding.spi.ShardingAuditAlgorithm;
import org.apache.shardingsphere.spring.namespace.factorybean.ShardingSphereAlgorithmFactoryBean;

import java.util.Properties;

/**
 * Sharding audit algorithm factory bean.
 */
public final class ShardingAuditAlgorithmFactoryBean extends ShardingSphereAlgorithmFactoryBean<ShardingAuditAlgorithm> {
    
    public ShardingAuditAlgorithmFactoryBean(final String type, final Properties props) {
        super(type, props, ShardingAuditAlgorithm.class);
    }
    
    @Override
    public ShardingAuditAlgorithm getObject() {
        return ShardingAuditAlgorithmFactory.newInstance(new AlgorithmConfiguration(getType(), getProps()));
    }
}
