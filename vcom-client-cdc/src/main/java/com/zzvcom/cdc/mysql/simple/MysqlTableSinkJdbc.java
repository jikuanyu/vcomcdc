package com.zzvcom.cdc.mysql.simple;

import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.util.conn.JdbcConnectionProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 目前效率低，不采用
 */
public class MysqlTableSinkJdbc extends RichSinkFunction<TableItem> {


    private static final Logger log = LoggerFactory.getLogger(MysqlTableSinkJdbc.class);

    /**
     * jdbc连接器提供者。
     */
    private final JdbcConnectionProvider connectionProvider;

    /**
     * 最大重试次数
     */
    private static final int MAX_TRY_NUM = 5;

    public MysqlTableSinkJdbc(JdbcConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
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
    public void invoke(TableItem value, SinkFunction.Context context) {
        for (int i = 0; i <= MAX_TRY_NUM; i++) {
            try {
                MysqlTableWriter.excSql(value, connectionProvider.getConnection());
                break;
            } catch (Exception e) {
                if (i >= MAX_TRY_NUM) {
                    throw FatalException.of("超过的重试次数无法继续。"+i);
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(500L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw FatalException.of(
                            "unable to flush; interrupted while doing another attempt", e);
                }
                try {
                    //重新获取连接
                    if (!connectionProvider.isConnectionValid()) {
                        connectionProvider.reestablishConnection();
                    }
                } catch (Exception ex) {
                   log.error("重连数据库异常：" + ex.getMessage()+",第"+i+"次", ex);
                }
            }
        }
    }
}
