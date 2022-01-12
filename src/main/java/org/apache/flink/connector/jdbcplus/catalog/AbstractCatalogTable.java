package org.apache.flink.connector.jdbcplus.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-01-10
 * @Description:
 */
public abstract class AbstractCatalogTable implements CatalogTable {
    // Schema of the table (column names and types)
    private final Schema tableSchema;
    // Partition keys if this is a partitioned table. It's an empty set if the table is not
    // partitioned
    private final List<String> partitionKeys;
    // Properties of the table
    private final Map<String, String> options;
    // Comment of the table
    private final String comment;

    public AbstractCatalogTable(
            Schema tableSchema, Map<String, String> options, String comment) {
        this(tableSchema, new ArrayList<>(), options, comment);
    }

    public AbstractCatalogTable(
            Schema tableSchema,
            List<String> partitionKeys,
            Map<String, String> options,
            String comment) {
        this.tableSchema = checkNotNull(tableSchema, "tableSchema cannot be null");
        this.partitionKeys = checkNotNull(partitionKeys, "partitionKeys cannot be null");
        this.options = checkNotNull(options, "options cannot be null");

        checkArgument(
                options.entrySet().stream()
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "properties cannot have null keys or values");

        this.comment = comment;
    }

    @Override
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    @Override
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public Schema getUnresolvedSchema() {
        return tableSchema;
    }

    @Override
    public String getComment() {
        return comment;
    }
}
