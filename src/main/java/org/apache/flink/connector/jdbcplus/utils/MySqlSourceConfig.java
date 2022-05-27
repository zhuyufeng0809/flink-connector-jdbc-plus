package org.apache.flink.connector.jdbcplus.utils;

import java.io.Serializable;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-04-29
 * @Description:
 */
public class MySqlSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final String databaseName;
    private final String tableName;
    private final int splitNum;
    private final int splitSize;
    private final String splitColumn;

    public MySqlSourceConfig(String hostname, int port, String username, String password, String databaseName, String tableName, int splitNum, int splitSize, String splitColumn) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.splitNum = splitNum;
        this.splitSize = splitSize;
        this.splitColumn = splitColumn;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getSplitNum() {
        return splitNum;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public String getSplitColumn() {
        return splitColumn;
    }
}
