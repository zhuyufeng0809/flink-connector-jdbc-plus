package org.apache.flink.connector.jdbcplus.hybrid;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.jdbcplus.hybrid.split.MySqlSnapshotSplit;
import org.apache.flink.connector.jdbcplus.hybrid.state.MysqlSplitEnumeratorState;
import org.apache.flink.connector.jdbcplus.utils.MySqlSourceConfig;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-04-29
 * @Description:
 */
public class MySqlSplitEnumerator implements SplitEnumerator<MySqlSnapshotSplit, MysqlSplitEnumeratorState> {

    //SplitEnumerator所处的上下文环境
    private final SplitEnumeratorContext<MySqlSnapshotSplit> context;
    private final MySqlSourceConfig mysqlConfig;

    public MySqlSplitEnumerator(SplitEnumeratorContext<MySqlSnapshotSplit> context, MySqlSourceConfig mysqlConfigs) {
        this.context = context;
        this.mysqlConfig = mysqlConfigs;

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {

    }

    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {

    }

    @Override
    public void addSplitsBack(List<MySqlSnapshotSplit> list, int i) {

    }

    @Override
    public void addReader(int i) {

    }

    @Override
    public MysqlSplitEnumeratorState snapshotState(long l) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
