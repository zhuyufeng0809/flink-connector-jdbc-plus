package org.apache.flink.connector.jdbcplus.catalog;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-01-15
 * @Description:
 */
public class CatalogNotExistException extends Exception {
    public CatalogNotExistException(String message) {
        super(message);
    }
}
