package com.zzvcom.cdc.mysql.cfg;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.setting.dialect.Props;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.zzvcom.cdc.cfg.BatchSinkOption;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.ConfigureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * mysqlToJdbcReader
 */
public class MysqlToJdbcReader {

    private static final Logger log = LoggerFactory.getLogger(MysqlToJdbcReader.class);

    private MysqlToJdbcReader() {
    }

    public static MysqlToJdbc readCfgFilePath(File filePath) throws ConfigureException {
        Props props = new Props(filePath, StandardCharsets.UTF_8);
        MysqlToJdbc mysqlToJdbc = new MysqlToJdbc();
        mysqlToJdbc.setHostname(props.getStr("hostname"));
        mysqlToJdbc.setPort(props.getInt("port", 3306));
        mysqlToJdbc.setUsername(props.getStr("username"));
        mysqlToJdbc.setPassword(props.getStr("password"));
        mysqlToJdbc.setDatabaseName(props.getStr("database-name"));
        mysqlToJdbc.setTableName(props.getStr("table-name"));
        mysqlToJdbc.setServerId(props.getStr("server-id"));
        mysqlToJdbc.setScanIncrementalSnapshot(props.getBool("scan.incremental.snapshot.enabled", true));
        mysqlToJdbc.setFetchSize(props.getInt("scan.snapshot.fetch.size", 1024));
        mysqlToJdbc.setSplitSize(props.getInt("scan.incremental.snapshot.chunk.size", 8096));
        mysqlToJdbc.setConnectTimeout(props.getLong("TestMysqlToJdbc.properties", 1800L));
        String startupMode = props.getStr("scan.startup.mode", "initial");
        StartupOptions startupOptions = null;
        if (StartupMode.INITIAL.name().toLowerCase().equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.initial();
        } else if (StartupMode.EARLIEST_OFFSET.name().equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.earliest();
        } else if (StartupMode.LATEST_OFFSET.name().equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.latest();
        } else if (StartupMode.TIMESTAMP.name().equalsIgnoreCase(startupMode)) {

            final String startTime = props.getStr("scan.startup.timestamp");
            if (startTime == null) {
                throw ConfigureException.of("scan.startup.mode" + StartupMode.TIMESTAMP + ",scan.startup.timestamp 必须给出(yyyy-MM-dd HH:mm:ss)格式的时间戳或者毫秒");
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                final Date parse = sdf.parse(startTime);
                startupOptions = StartupOptions.timestamp(parse.getTime());
            } catch (ParseException e) {
                log.warn("startTime={}转换失败,尝试直接使用", startTime);
                startupOptions = StartupOptions.timestamp(Long.valueOf(startTime));
            }
        } else if (StartupMode.SPECIFIC_OFFSETS.name().equalsIgnoreCase(startupMode)) {
            //#'mysql-bin.000003', -- 在特定位点启动模式下指定 binlog 文件名scan.startup.specific-offset.file=
            final String offsetFile = props.getStr("scan.startup.specific-offset.file");
            //#'4', -- 在特定位点启动模式下指定 binlog 位置
            final Long pos = props.getLong("scan.startup.specific-offset.pos");
            //#'24DA167-0C0C-11E8-8442-00059A3C7B00:1-19', -- 在特定位点启动模式下指定 GTID 集合
            final String gtidSet = props.getStr("scan.startup.specific-offset.gtid-set");
            BinlogOffset build = null;
            if (CharSequenceUtil.isNotBlank(offsetFile) && pos != null) {
                build = BinlogOffset.builder().setBinlogFilePosition(offsetFile, pos).build();
            } else if (CharSequenceUtil.isNotBlank(gtidSet)) {
                // .startupOptions(StartupOptions.specificOffset("24DA167-0C0C-11E8-8442-00059A3C7B00:1-19")) // 从 GTID 集合启动
                build = BinlogOffset.builder().setGtidSet(gtidSet).build();
            } else {
                log.error("scan.startup.specific-offset.file={},scan.startup.specific-offset.pos={} or scan.startup.specific-offset.gtid-set={}",
                        offsetFile, pos, gtidSet);
                throw ConfigureException.of("SPECIFIC_OFFSETS 设置不正确");
            }
            startupOptions = StartupOptions.specificOffset(build);
        } else {
            throw ConfigureException.of("startupOptions未支持类型");
        }
        mysqlToJdbc.setStartupOptions(startupOptions);
        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl(props.getStr("dest.jdbc.url"));
        jdbcInfo.setUserName(props.getStr("dest.jdbc.username"));
        jdbcInfo.setPassword(props.getStr("dest.jdbc.password"));
        jdbcInfo.setDriverClass(props.getStr("dest.jdbc.driver"));
        mysqlToJdbc.setJdbcInfo(jdbcInfo);

        BatchSinkOption batchSinkOption = new BatchSinkOption();
        //每批插入最大大小dest.jdbc.batch.batchSize=5000
        batchSinkOption.setBatchSize(props.getInt("dest.jdbc.batch.batchSize", 5000));
        //执行异常的最大重试次数 dest.jdbc.batch.maxTryNum=3
        batchSinkOption.setMaxTryNum(props.getInt("dest.jdbc.batch.maxTryNum", 3));
        //批量定时的频率dest.jdbc.batch.batchIntervalMs=2000
        batchSinkOption.setBatchIntervalMs(props.getLong("dest.jdbc.batch.batchIntervalMs", 2000L));

        //# 1、直接模式 2生产者消费者模式
        //dest.jdbc.batch.model=2
        //#2生产消费者模式的时候,缓存的容量多少批。
        //dest.jdbc.batch.model.batchBuffSize=3
        batchSinkOption.setModel(props.getInt("dest.jdbc.batch.model", 2));
        batchSinkOption.setModelBatchBuffSize(props.getInt("dest.jdbc.batch.model.batchBuffSize", 3));
        jdbcInfo.setBatchSinkOption(batchSinkOption);
        return mysqlToJdbc;
    }

}
