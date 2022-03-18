package org.apache.shardingsphere.infra.metadata.schema.builder;

import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.schema.builder.spi.DialectSystemSchemaBuilder;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public final class MySQLSystemSchemaBuilder implements DialectSystemSchemaBuilder {
    
    @Override
    public String getDatabaseType() {
        return "MySQL";
    }
    
    @Override
    public Map<String, ShardingSphereSchema> build() {
        Map<String, ShardingSphereSchema> result = new LinkedHashMap<>();
        DatabaseType databaseType = DatabaseTypeRegistry.getTrunkDatabaseType(getDatabaseType());
        for (String each : databaseType.getSystemSchemas()) {
            // TODO build ShardingSphereSchema according to schema, this logic just for test
            Map<String, TableMetaData> tables = new HashMap<>();
            Collection<ColumnMetaData> columnMetaDataList = new LinkedList<>();
            columnMetaDataList.add(new ColumnMetaData("TABLE_CATALOG", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("TABLE_SCHEMA", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("TABLE_NAME", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("TABLE_TYPE", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("ENGINE", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("VERSION", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("ROW_FORMAT", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("TABLE_ROWS", Types.VARCHAR, false, false, false));
            columnMetaDataList.add(new ColumnMetaData("AVG_ROW_LENGTH", Types.VARCHAR, false, false, false));
            tables.put("TABLES", new TableMetaData("TABLES", columnMetaDataList, Collections.emptyList(), Collections.emptyList()));
            result.put(each, new ShardingSphereSchema(tables));
        }
        return result;
    }
}
