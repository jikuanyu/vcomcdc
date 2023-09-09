package com.zzvcom.cdc.oceanbase;

import com.ververica.cdc.connectors.oceanbase.table.OceanBaseRecord;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.jdbc.JdbcTable;
import com.zzvcom.cdc.oceanbase.sink.OceanBaseRecordItem;
import com.zzvcom.cdc.oceanbase.sink.OceanBaseRecordItemWriter;
import com.zzvcom.cdc.util.conn.JdbcConnectionProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OceanBaseSinkJDBC extends RichSinkFunction<OceanBaseRecord> {

    private static final Logger log = LoggerFactory.getLogger(OceanBaseSinkJDBC.class);
    /**
     * jdbc连接器提供者。
     */
    private final JdbcConnectionProvider connectionProvider;
    /**
     * 同步库目标的表结构，目前oceanbase log 没有办法获取表结构,所以进行了目标库的获取。
     */
    private final Map<String, JdbcTable> dbTableMap;
    /**
     * 最大重试次数
     */
    private static final int MAX_TRY_NUM = 5;


    public OceanBaseSinkJDBC(JdbcConnectionProvider connectionProvider, Map<String, JdbcTable> dbTableMap) {
        this.connectionProvider = connectionProvider;
        this.dbTableMap = dbTableMap;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new IOException("unable to open JDBC writer", e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        connectionProvider.closeConnection();
    }

    @Override
    public void invoke(OceanBaseRecord value, SinkFunction.Context context) {

        for (int i = 0; i <= MAX_TRY_NUM; i++) {
            try {  //进行OceanBaseRecordItem
                OceanBaseRecordItem item = new OceanBaseRecordItem(value);
                //数据结构
                JdbcTable jdbcTable = dbTableMap.get(item.getRecord().getSourceInfo().getDatabase() + "." + item.getRecord().getSourceInfo().getTable());
                if (jdbcTable == null) {
                    log.error("没有找到对应的目标表接口，忽略数据,item={}", item);
                    return;
                }
                item.setJdbcTable(jdbcTable);
                log.warn("item={}", item);
                OceanBaseRecordItemWriter.excSql(item, connectionProvider.getConnection());
                break;
            } catch (Exception e) {
                log.error("JDBC executeBatch error, retry times = {}", i, e);
                log.error("invoke Exception", e);
                log.error("invoke tableItem ={}", value);
                try {
                    TimeUnit.MILLISECONDS.sleep(500L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new FatalException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
                try {
                    //如果连接是无效的，重新获取连接
                    if (!connectionProvider.isConnectionValid()) {
                        connectionProvider.reestablishConnection();
                    }

                } catch (Exception ex) {
                    log.error("JDBC connection is not valid, and reestablish connection failed.", ex);
                }
            }
        }
    }
}
