package org.apache.flink.connector.jdbcplus.catalog;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.Map;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-12-28
 * @Description:
 */
public abstract class AbstractGenerator {

    public abstract Catalog createCatalog(CatalogName catalogName);

    public abstract void createTable(CatalogName catalogName, ObjectPath tablePath, Map<String, String> props) throws Exception;
}
