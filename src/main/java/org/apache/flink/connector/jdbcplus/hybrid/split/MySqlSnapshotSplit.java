package org.apache.flink.connector.jdbcplus.hybrid.split;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-04-29
 * @Description:
 */
public class MySqlSnapshotSplit implements SourceSplit {

    private final int lowerBound;
    private final int upperBound;

    public MySqlSnapshotSplit(int lowerBound, int upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public String splitId() {
        return lowerBound + "-" + upperBound;
    }
}
