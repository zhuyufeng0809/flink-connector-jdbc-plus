package org.apache.flink.connector.jdbcplus.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.*;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-01-10
 * @Description:
 */
public class MysqlCatalogTable extends AbstractCatalogTable{

    public MysqlCatalogTable(
            Schema tableSchema, Map<String, String> properties, String comment) {
        this(tableSchema, new ArrayList<>(), properties, comment);
    }

    public MysqlCatalogTable(
            Schema tableSchema,
            List<String> partitionKeys,
            Map<String, String> properties,
            String comment) {
        super(tableSchema, partitionKeys, properties, comment);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new MysqlCatalogTable(getUnresolvedSchema(), getPartitionKeys(), options, getComment());
    }

    @Override
    public CatalogBaseTable copy() {
        Schema schema = Schema.newBuilder().fromSchema(getUnresolvedSchema()).build();

        return new MysqlCatalogTable(
                schema,
                new ArrayList<>(getPartitionKeys()),
                new HashMap<>(getOptions()),
                getComment());
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.of("This is a catalog table in an im-memory catalog");
    }


}
