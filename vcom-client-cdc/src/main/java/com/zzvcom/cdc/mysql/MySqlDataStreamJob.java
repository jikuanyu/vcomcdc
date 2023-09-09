

package com.zzvcom.cdc.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zzvcom.cdc.cfg.BatchSinkOption;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.ConfigureException;
import com.zzvcom.cdc.mysql.cfg.MysqlToJdbc;
import com.zzvcom.cdc.mysql.cfg.MysqlToJdbcReader;
import com.zzvcom.cdc.mysql.simple.TableItem;
import com.zzvcom.cdc.mysql.sink.batch.BatchJdbcWriter;
import com.zzvcom.cdc.mysql.sink.batch.BatchTableSinkJdbc;
import com.zzvcom.cdc.mysql.sink.batch.buff.BatchBufferJdbcWriter;
import com.zzvcom.cdc.mysql.sink.batch.buff.ds.DsBatchBufferJdbcWriter;
import com.zzvcom.cdc.mysql.sink.batch.direct.SimpleBatchJdbcWriter;
import com.zzvcom.cdc.util.conn.VcomJdbcConnectionProvider;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

/**
 * mysql cdc同步到其他数据库，使用jdbc接受的方式
 * @author yujikuan
 */
public class MySqlDataStreamJob {
    private static final Logger log = LoggerFactory.getLogger(MySqlDataStreamJob.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw ConfigureException.of("必须传递一个配置文件");
        }
        File file = new File(args[0]);
        log.debug("file={}", file);
        if (!file.exists()) {
            throw ConfigureException.of("文件不存在,file=" + file.getAbsolutePath());
        }
        //读取mysql同步的配置文件
        MysqlToJdbc mysqlToJdbc = MysqlToJdbcReader.readCfgFilePath(file);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint 5分钟
        env.enableCheckpointing(300 * 1000L);
        MySqlSource<TableItem> mySqlSource = MySqlSource.<TableItem>builder()
                .hostname(mysqlToJdbc.getHostname())
                .port(mysqlToJdbc.getPort())
                .databaseList(mysqlToJdbc.getDatabaseName())
                .tableList(mysqlToJdbc.getTableName())
                .username(mysqlToJdbc.getUsername())
                .password(mysqlToJdbc.getPassword())
                .scanNewlyAddedTableEnabled(true)
                .splitSize(mysqlToJdbc.getSplitSize())
                .fetchSize(mysqlToJdbc.getFetchSize())
                .deserializer(new TableItemDeserializationSchema())
                .startupOptions(mysqlToJdbc.getStartupOptions())
                .connectTimeout(Duration.ofSeconds(mysqlToJdbc.getConnectTimeout()))
                .build();
        JdbcInfo jdbcInfo = mysqlToJdbc.getJdbcInfo();
        BatchJdbcWriter batchJdbcWriter;
        if (BatchSinkOption.MODEL_DIRECT == mysqlToJdbc.getJdbcInfo().getBatchSinkOption().getModel()) {
            batchJdbcWriter = new SimpleBatchJdbcWriter(new VcomJdbcConnectionProvider(jdbcInfo));
        } else if (BatchSinkOption.MODEL_BUFF == mysqlToJdbc.getJdbcInfo().getBatchSinkOption().getModel()) {
            batchJdbcWriter = new BatchBufferJdbcWriter(new VcomJdbcConnectionProvider(jdbcInfo));
        } else if (BatchSinkOption.MODEL_BUFF_POOL == mysqlToJdbc.getJdbcInfo().getBatchSinkOption().getModel()) {
            batchJdbcWriter = new DsBatchBufferJdbcWriter(jdbcInfo);
        } else {
            throw ConfigureException.of("未知模式"+mysqlToJdbc.getJdbcInfo().getBatchSinkOption().getModel());
        }

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqltojdbcbatch")
                .addSink(new BatchTableSinkJdbc(batchJdbcWriter));
        env.execute("mysqlcdc,cfg=" + file.getAbsolutePath());
    }
}
