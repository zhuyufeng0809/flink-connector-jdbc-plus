package org.apache.flink.connector.jdbcplus.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static org.apache.flink.connector.jdbcplus.table.JdbcConnectorOptions.*;
import static org.apache.flink.connector.jdbcplus.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbcplus.table.JdbcPlusDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-01-10
 * @Description:
 */
public class MysqlCatalog extends AbstractJdbcCatalog{

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCatalog.class);

    protected MysqlCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> schemas = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getCatalogs();

            while (rs.next()) {
                schemas.add(rs.getString("TABLE_CAT"));
            }

            return schemas;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> tables = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs =  metaData.getTables(databaseName, null, null, new String[]{"TABLE", "VIEW", "SYSTEM TABLE"});

            while (rs.next()) {
                tables.add(databaseName + "." + rs.getString("TABLE_NAME"));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String dbUrl = baseUrl + tablePath.getDatabaseName();

        try(Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            String catalog = tablePath.getDatabaseName();
            String tableName = tablePath.getObjectName();
            List<String> primaryKeys = getFlinkPrimaryKey(metaData, catalog, tableName);
            String tableComment = getTableComment(metaData, catalog, tableName);

            PreparedStatement ps =
                    conn.prepareStatement(String.format("SELECT * FROM %s;", tablePath.getFullName()));

            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            String[] names = new String[resultSetMetaData.getColumnCount()];
            DataType[] types = new DataType[resultSetMetaData.getColumnCount()];

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                names[i - 1] = resultSetMetaData.getColumnName(i);
                types[i - 1] = fromJDBCType(resultSetMetaData, i);

                //primary fields must not null, otherwise Schema.resolve report errors
                //Schema.resolve will check whether primary fields is not null
                if (primaryKeys.contains(names[i - 1])) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }

            Schema tableSchema = Schema.newBuilder()
                    .fromFields(names, types)
                    .primaryKeyNamed("idx_PK", primaryKeys)
                    .withComment(tableComment)
                    .build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), dbUrl);
            props.put(TABLE_NAME.key(), tablePath.getFullName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);
            props.put(DRIVER.key(), "com.mysql.cj.jdbc.Driver");

            return new MysqlCatalogTable(tableSchema, props, tableComment);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables;

        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }

        return tables.contains(tablePath.getFullName());
    }

    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        //reference Flink official website
        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/jdbc/#data-type-mapping
        switch (mysqlType) {
            case MysqlDataType.MYSQL_TINYINT:
                if (precision == 1) {
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case MysqlDataType.MYSQL_SMALLINT:
            case MysqlDataType.MYSQL_TINYINT_UNSIGNED:
                return DataTypes.SMALLINT();
            case MysqlDataType.MYSQL_INT:
            case MysqlDataType.MYSQL_MEDIUMINT:
            case MysqlDataType.MYSQL_SMALLINT_UNSIGNED:
                return DataTypes.INT();
            case MysqlDataType.MYSQL_BIGINT:
            case MysqlDataType.MYSQL_INT_UNSIGNED:
                return DataTypes.BIGINT();
            case MysqlDataType.MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MysqlDataType.MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MysqlDataType.MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MysqlDataType.MYSQL_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MysqlDataType.MYSQL_BOOLEAN:
                return DataTypes.BOOLEAN();
            case MysqlDataType.MYSQL_DATE:
                return DataTypes.DATE();
            case MysqlDataType.MYSQL_TIME:
                return DataTypes.TIME(scale);
            case MysqlDataType.MYSQL_DATETIME:
                return DataTypes.TIMESTAMP(scale);
            case MysqlDataType.MYSQL_CHAR:
            case MysqlDataType.MYSQL_VARCHAR:
            case MysqlDataType.MYSQL_TEXT:
                return DataTypes.STRING();
            case MysqlDataType.MYSQL_BINARY:
            case MysqlDataType.MYSQL_VARBINARY:
            case MysqlDataType.MYSQL_BLOB:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Mysql type ''%s yet, please contact author", mysqlType));
        }
    }


    private List<String> getFlinkPrimaryKey(DatabaseMetaData metaData, String schema, String table) throws SQLException {
        List<String> uniqueKeys = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();

        ResultSet idxUnique = metaData.getIndexInfo(schema, null, table, true, false);
        ResultSet idxPrimary = metaData.getPrimaryKeys(schema, null, table);

        while (idxUnique.next()) {
            uniqueKeys.add(idxUnique.getString("COLUMN_NAME"));
        }

        while (idxPrimary.next()) {
            primaryKeys.add(idxPrimary.getString("COLUMN_NAME"));
        }

        //prefer to use uniqueKey
        uniqueKeys.removeAll(primaryKeys);

        if (uniqueKeys.size() == 0) {
            return primaryKeys;
        } else {
            return uniqueKeys;
        }
    }

    private String getTableComment(DatabaseMetaData metaData, String schema, String table) throws SQLException {
        String tableComment = "";

        ResultSet rs = metaData.getTables(schema, null, table, new String[]{"TABLE", "VIEW", "SYSTEM TABLE"});

        while (rs.next()) {
            tableComment = rs.getString("REMARKS");
        }

        return tableComment;
    }
}
