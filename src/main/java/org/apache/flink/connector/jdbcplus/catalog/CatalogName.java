package org.apache.flink.connector.jdbcplus.catalog;

import java.util.Objects;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-01-14
 * @Description:
 */
public final class CatalogName extends AvailableCatalogName {

    private final String catalogName;

    private CatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public static CatalogName getOdsCatalogName() {
        return new CatalogName(ODS_CATALOG_NAME);
    }

    public static CatalogName getCdmCatalogName() {
        return new CatalogName(CDM_CATALOG_NAME);
    }

    public static CatalogName getAdsCatalogName() {
        return new CatalogName(ADS_CATALOG_NAME);
    }

    @Override
    public String toString() {
        return catalogName;
    }

    //must override hashCode method
    @Override
    public int hashCode() {
        return Objects.hash(catalogName);
    }

    //must override equals method
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CatalogName that = (CatalogName) obj;

        return Objects.equals(catalogName, that.catalogName);
    }
}
