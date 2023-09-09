package com.zzvcom.cdc.oceanbase;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.text.CharSequenceUtil;
import com.ververica.cdc.connectors.oceanbase.OceanBaseSource;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseRecord;
import com.ververica.cdc.connectors.oceanbase.table.StartupMode;
import com.zzvcom.cdc.cfg.BatchSinkOption;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.ConfigureException;
import com.zzvcom.cdc.jdbc.JdbcParserMata;
import com.zzvcom.cdc.jdbc.JdbcTable;
import com.zzvcom.cdc.oceanbase.cfg.OceanbaseToJdbc;
import com.zzvcom.cdc.oceanbase.cfg.OceanbaseToJdbcReader;
import com.zzvcom.cdc.oceanbase.sink.batch.OceanBaseBatchSinkJDBC;
import com.zzvcom.cdc.oceanbase.sink.batch.ds.OceanBaseDsBatchBufferJdbcWriter;
import com.zzvcom.cdc.util.conn.VcomJdbcConnectionProvider;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;


public class OceanBaseDataStreamJob {


    private static final Logger log = LoggerFactory.getLogger(OceanBaseDataStreamJob.class);

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            throw ConfigureException.of("必须传递一个配置文件");
        }
        File file = new File(args[0]);
        log.debug("file={}", file);
        if (!file.exists()) {
            throw ConfigureException.of("文件不存在,file=" + file.getAbsolutePath());
        }
        OceanbaseToJdbc oceanbaseToJdbc = OceanbaseToJdbcReader.readCfgFilePath(file.getAbsolutePath());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<OceanBaseRecord> oceanBaseSource =
                OceanBaseSource.<OceanBaseRecord>builder()
                        .rsList(oceanbaseToJdbc.getRootserverList())
                        .startupMode(StartupMode.getStartupMode(oceanbaseToJdbc.getStartupMode()))
                        .startupTimestamp(oceanbaseToJdbc.getStartupTimestamp())
                        .username(oceanbaseToJdbc.getUsername())
                        .password(oceanbaseToJdbc.getPassword())
                        .tenantName(oceanbaseToJdbc.getTenantName())
                        .tableList(oceanbaseToJdbc.getTableList())
                        .databaseName(oceanbaseToJdbc.getDatabaseName())
                        .tableName(oceanbaseToJdbc.getTableName())
                        .hostname(oceanbaseToJdbc.getHostname())
                        .port(oceanbaseToJdbc.getPort())
                        .compatibleMode(oceanbaseToJdbc.getCompatibleMode())
                        .jdbcDriver(oceanbaseToJdbc.getJdbcDriver())
                        .logProxyHost(oceanbaseToJdbc.getLogProxyHost())
                        .logProxyPort(oceanbaseToJdbc.getLogproxyPort())
                        .serverTimeZone(oceanbaseToJdbc.getServerTimeZone())
                        .deserializer(new OceanBaseDataSchema())
                        .connectTimeout(Duration.ofSeconds(oceanbaseToJdbc.getConnectTimeout()))
                        .build();
        // enable checkpoint 5分钟
        env.enableCheckpointing(300 * 1000L);
        //输入端jdbcinfo
        JdbcInfo jdbcInfo = oceanbaseToJdbc.getDestJdbc();
        log.debug("oceanbaseToJdbc={}", oceanbaseToJdbc);
        //提取目标库的表结构
        Map<String, JdbcTable> dbTableMap = null;
        if (CollUtil.isEmpty(oceanbaseToJdbc.getAllDbs())) {
            //表达式方式
            dbTableMap = JdbcParserMata.parserMataMapByJdbcInfo(jdbcInfo, oceanbaseToJdbc.getDatabaseName(), oceanbaseToJdbc.getTableName());
        } else {
            final List<String> tables = CharSequenceUtil.split(oceanbaseToJdbc.getTableList(), ",");
            dbTableMap = JdbcParserMata.parserMataMapByJdbcInfo(jdbcInfo, oceanbaseToJdbc.getAllDbs(), tables);
        }
        log.warn("dbTableMap={}", dbTableMap);
        if (jdbcInfo.getBatchSinkOption().getModel() == BatchSinkOption.MODEL_DIRECT) {
            env.addSource(oceanBaseSource).addSink(new OceanBaseSinkJDBC(new VcomJdbcConnectionProvider(jdbcInfo), dbTableMap)).name("oceanbaseToJdbc");
        } else {
            env.addSource(oceanBaseSource).addSink(new OceanBaseBatchSinkJDBC(new OceanBaseDsBatchBufferJdbcWriter(jdbcInfo), dbTableMap)).name("batchoceanbaseToJdbc");
        }
        env.execute("oceanbasecdc" + oceanbaseToJdbc.getAllDbs() + ",cfg=" + file.getAbsolutePath());
    }

}
