package org.apache.flink.connector.jdbcplus.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
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
public class MysqlCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCatalog.class);
    private final Map<String, CatalogDatabase> databases;
    private final Map<ObjectPath, CatalogBaseTable> tables;
    private final String jdbcUrlPara;

    protected MysqlCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl,
            String jdbcUrlPara) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);

        if (jdbcUrlPara == null) {
            this.jdbcUrlPara = "";
        } else {
            this.jdbcUrlPara = jdbcUrlPara.length() == 0 ? jdbcUrlPara : "?" + jdbcUrlPara;
        }

        this.databases = new LinkedHashMap<>();
        this.tables = new LinkedHashMap<>();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        //avoid connect database on each call
        if (databases.size() != 0) {
            return new ArrayList<>(databases.keySet());
        } else {
            //first call this method
            List<String> schemas = new ArrayList<>();

            try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs = metaData.getCatalogs();

                while (rs.next()) {
                    String schema = rs.getString("TABLE_CAT");
                    schemas.add(schema);
                    this.databases.put(schema, new CatalogDatabaseImpl(Collections.emptyMap(), null));
                }

                return schemas;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed listing database in catalog %s", getName()), e);
            }
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } else {
            //attention:deep copy
            return databases.get(databaseName).copy();
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        List<String> tableNames = new ArrayList<>();

        for (ObjectPath path : tables.keySet()) {
            if (path.getDatabaseName().equals(databaseName)) {
                tableNames.add(path.getFullName());
            }
        }

        if (tables.size() != 0 && tableNames.size() != 0) {
            return tableNames;
        } else {
            try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs =  metaData.getTables(databaseName, null, null, new String[]{"TABLE", "VIEW", "SYSTEM TABLE"});

                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    tableNames.add(databaseName + "." + tableName);
                    tables.put(new ObjectPath(databaseName, tableName), null);
                }

                return tableNames;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed listing database in catalog %s", getName()), e);
            }
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        CatalogBaseTable table = tables.get(tablePath);

        if (table != null) {
            return table;
        } else {
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
                props.put(URL.key(), dbUrl + this.jdbcUrlPara);
                props.put(TABLE_NAME.key(), tablePath.getObjectName());
                props.put(USERNAME.key(), username);
                props.put(PASSWORD.key(), pwd);
                props.put(DRIVER.key(), "com.mysql.cj.jdbc.Driver");

                CatalogBaseTable catalogTable = new MysqlCatalogTable(tableSchema, props, tableComment);
                tables.put(tablePath, catalogTable);

                return catalogTable;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed getting table %s", tablePath.getFullName()), e);
            }
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        tables.put(tablePath, table.copy());
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

//    public Map<String, String> getMinAndMaxBound(String sql) {
//        HashMap<String, String> result = new HashMap<>();
//
//        try(Connection conn = DriverManager.getConnection(baseUrl, username, pwd)) {
//            PreparedStatement statement = conn.prepareStatement(sql);
//            ResultSet resultSet = statement.executeQuery();
//
//            resultSet.next();
//            result.put("lower-bound", resultSet.getString(1));
//            result.put("upper-bound", resultSet.getString(2));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return result;
//    }

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
                //type 'BIT' reference PolarDb Mysql official website
                //https://help.aliyun.com/document_detail/131282.htm?spm=a2c4g.11186623.0.0.4f86739cjdzqAQ#concept-1813784
            case MysqlDataType.MYSQL_BIT:
                return DataTypes.STRING();
            case MysqlDataType.MYSQL_BINARY:
            case MysqlDataType.MYSQL_VARBINARY:
            case MysqlDataType.MYSQL_BLOB:
                return DataTypes.BYTES();
            default:
                String columnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Mysql type '%s' of column '%s' yet, please contact author", mysqlType,columnName));
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
