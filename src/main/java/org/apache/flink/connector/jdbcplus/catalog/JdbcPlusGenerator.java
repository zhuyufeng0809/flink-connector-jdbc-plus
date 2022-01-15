package org.apache.flink.connector.jdbcplus.catalog;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-01-14
 * @Description:
 */
public final class JdbcPlusGenerator extends AbstractGenerator {

    private final ParameterTool cmd;
    private final ParameterTool property;
    private final boolean isStreamingMode;
    private final Map<CatalogName, Catalog> catalogs;

    private JdbcPlusGenerator(ParameterTool config, boolean isStreamingMode) throws Exception {
        this.cmd = config;
        this.property = ParameterTool.fromPropertiesFile(getProperty("conf"));
        this.isStreamingMode = isStreamingMode;
        this.catalogs = new HashMap<>();
    }

    @Override
    public Catalog createCatalog(CatalogName catalogName) {
        String currentCatalogName = catalogName.toString();
        String jdbcUrlPrefix = "jdbc:mysql://";
        String jdbcUrlPort = ":3306";

        switch (currentCatalogName) {
            case CatalogName.ODS_CATALOG_NAME:
                Catalog odsCatalog = new JdbcCatalog(
                        currentCatalogName,
                        getProperty("etlpolar.default_db"),
                        getProperty("etlpolar.username"),
                        getProperty("etlpolar.password"),
                        jdbcUrlPrefix + getProperty("etlpolar.host") + jdbcUrlPort,
                        isStreamingMode ? getProperty("jdbc.params.polar.stream") : getProperty("jdbc.params.polar.batch")
                );
                this.catalogs.put(catalogName, odsCatalog);
                return odsCatalog;
            case CatalogName.CDM_CATALOG_NAME:
                Catalog cdmCatalog = new JdbcCatalog(
                        currentCatalogName,
                        getProperty("etladb.default_db"),
                        getProperty("etladb.username"),
                        getProperty("etladb.password"),
                        jdbcUrlPrefix + getProperty("etladb.host") + jdbcUrlPort,
                        isStreamingMode ? getProperty("jdbc.params.adb.stream") : getProperty("jdbc.params.adb.batch")
                );
                this.catalogs.put(catalogName, cdmCatalog);
                return cdmCatalog;
            case CatalogName.ADS_CATALOG_NAME:
                Catalog adsCatalog = new JdbcCatalog(
                        currentCatalogName,
                        getProperty("pioneeradb.default_db"),
                        getProperty("pioneeradb.username"),
                        getProperty("pioneeradb.password"),
                        jdbcUrlPrefix + getProperty("pioneeradb.host") + jdbcUrlPort,
                        isStreamingMode ? getProperty("jdbc.params.adb.stream") : getProperty("jdbc.params.adb.batch")
                );
                this.catalogs.put(catalogName, adsCatalog);
                return adsCatalog;
            default:
                return null;
        }
    }

    @Override
    public void createTable(CatalogName catalogName, ObjectPath tablePath, Map<String, String> props) throws Exception {
        JdbcCatalog catalog = (JdbcCatalog) catalogs.get(catalogName);

        if (catalog == null) {
            throw new CatalogNotExistException(
                    String.format("catalog %s does not exist", catalogName.toString())
            );
        } else {
            CatalogTable catalogTable = (CatalogTable) catalog.getTable(tablePath);
            Map<String, String> defaultProps = catalogTable.getOptions();
            defaultProps.putAll(props);
            catalog.createTable(tablePath, catalogTable.copy(defaultProps), false);
        }
    }

    public String getProperty(String key) {
        String result = cmd.get(key);

        if (result == null) {
            return property.get(key);
        } else {
            return result;
        }
    }

    public static JdbcPlusGenerator.Builder newBuilder() {
        return new JdbcPlusGenerator.Builder();
    }

    public static final class Builder {

        private ParameterTool config;

        private boolean isStreamingModeInternal;

        public JdbcPlusGenerator.Builder fromConfig(String[] args, boolean isStreamingMode) {
            config = ParameterTool.fromArgs(args);
            isStreamingModeInternal = isStreamingMode;
            return this;
        }

        public JdbcPlusGenerator build() throws Exception {
            return new JdbcPlusGenerator(config, isStreamingModeInternal);
        }
    }
}
