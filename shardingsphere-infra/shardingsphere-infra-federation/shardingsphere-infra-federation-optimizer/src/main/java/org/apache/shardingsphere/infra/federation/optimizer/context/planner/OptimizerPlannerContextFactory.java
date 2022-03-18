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

package org.apache.shardingsphere.infra.federation.optimizer.context.planner;

import com.sun.org.apache.bcel.internal.generic.FADD;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.shardingsphere.infra.federation.optimizer.metadata.FederationMetaData;
import org.apache.shardingsphere.infra.federation.optimizer.metadata.FederationSchemaMetaData;
import org.apache.shardingsphere.infra.federation.optimizer.metadata.calcite.FederationSchema;
import org.apache.shardingsphere.infra.federation.optimizer.planner.QueryOptimizePlannerFactory;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Optimizer planner context factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OptimizerPlannerContextFactory {
    
    /**
     * Create optimizer planner context map.
     *
     * @param metaData federation meta data
     * @return created optimizer planner context map
     */
    public static Map<String, OptimizerPlannerContext> create(final FederationMetaData metaData) {
        metaData.getSchemas().put("postgres", new FederationSchemaMetaData("postgres", Collections.emptyMap()));
        Map<String, OptimizerPlannerContext> result = new HashMap<>(metaData.getSchemas().size(), 1);
        for (Entry<String, FederationSchemaMetaData> entry : metaData.getSchemas().entrySet()) {
            String schemaName = entry.getKey();
            FederationSchema federationSchema = new FederationSchema(entry.getValue());
            CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(createConnectionProperties());
            RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();
            CalciteCatalogReader catalogReader = createCatalogReader(schemaName, federationSchema, relDataTypeFactory, connectionConfig);
            SqlValidator validator = createValidator(catalogReader, relDataTypeFactory, connectionConfig);
            SqlToRelConverter converter = createConverter(catalogReader, validator, relDataTypeFactory);
            result.put(schemaName, new OptimizerPlannerContext(validator, converter));
        }
        return result;
    }
    
    /**
     * Create optimizer planner context.
     *
     * @param schemaMetaData federation schema meta data
     * @return created optimizer planner context
     */
    public static OptimizerPlannerContext create(final FederationSchemaMetaData schemaMetaData) {
        FederationSchema federationSchema = new FederationSchema(schemaMetaData);
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(createConnectionProperties());
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();
        CalciteCatalogReader catalogReader = createCatalogReader(schemaMetaData.getName(), federationSchema, relDataTypeFactory, connectionConfig);
        SqlValidator validator = createValidator(catalogReader, relDataTypeFactory, connectionConfig);
        SqlToRelConverter converter = createConverter(catalogReader, validator, relDataTypeFactory);
        return new OptimizerPlannerContext(validator, converter);
    }
    
    private static Properties createConnectionProperties() {
        Properties result = new Properties();
        result.setProperty(CalciteConnectionProperty.TIME_ZONE.camelName(), "UTC");
        return result;
    }
    
    private static CalciteCatalogReader createCatalogReader(final String schemaName, 
                                                            final Schema schema, final RelDataTypeFactory relDataTypeFactory, final CalciteConnectionConfig connectionConfig) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        rootSchema.add(schemaName, schema);
        CalciteSchema subSchema = rootSchema.getSubSchema(schemaName, false);
        for (String each : schema.getSubSchemaNames()) {
            if (null != subSchema) {
                subSchema.add(each, schema.getSubSchema(each));
            }
        }
        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList(schemaName), relDataTypeFactory, connectionConfig);
        return calciteCatalogReader.withSchemaPath(Arrays.asList("postgres", "information_schema"));
    }
    
    private static SqlValidator createValidator(final CalciteCatalogReader catalogReader, final RelDataTypeFactory relDataTypeFactory, final CalciteConnectionConfig connectionConfig) {
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withSqlConformance(connectionConfig.conformance())
                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                .withIdentifierExpansion(true);
        return SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, relDataTypeFactory, validatorConfig);
    }
    
    private static SqlToRelConverter createConverter(final CalciteCatalogReader catalogReader, final SqlValidator validator, final RelDataTypeFactory relDataTypeFactory) {
        ViewExpander expander = (rowType, queryString, schemaPath, viewPath) -> null;
        Config converterConfig = SqlToRelConverter.config().withTrimUnusedFields(true);
        RelOptCluster cluster = RelOptCluster.create(QueryOptimizePlannerFactory.newInstance(), new RexBuilder(relDataTypeFactory));
        return new SqlToRelConverter(expander, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, converterConfig);
    }
}
