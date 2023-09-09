package com.zzvcom.cdc.oceanbase.cfg;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.setting.dialect.Props;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.zzvcom.cdc.cfg.BatchSinkOption;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.ConfigureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class OceanbaseToJdbcReader {

    private static final Logger log = LoggerFactory.getLogger(OceanbaseToJdbcReader.class);

    private OceanbaseToJdbcReader() {
    }


    public static OceanbaseToJdbc readCfgFilePath(String filePath) throws ConfigureException {
        Props props = new Props(filePath, StandardCharsets.UTF_8);
        OceanbaseToJdbc item = new OceanbaseToJdbc();
        //源库的设置
        item.setRootserverList(props.getStr("rootserver-list"));
        item.setStartupMode(props.getStr("startup.mode"));
        //scan.startup.timestamp
        if (item.getStartupMode().equalsIgnoreCase("timestamp")) {
            Long timestamp = null;
            final String startTime = props.getStr("scan.startup.timestamp");
            if (startTime == null) {
                throw ConfigureException.of("startup.mode" + StartupMode.TIMESTAMP + ",scan.startup.timestamp 必须给出(yyyy-MM-dd HH:mm:ss)格式的时间戳或者毫秒");
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                final Date parse = sdf.parse(startTime);
                timestamp = parse.getTime();
            } catch (ParseException e) {
                log.warn("startTime={}转换失败,尝试直接使用", startTime);
                timestamp = Long.valueOf(startTime);
            }
            item.setStartupTimestamp(timestamp);
        }


        item.setUsername(props.getStr("username"));
        item.setPassword(props.getStr("password"));
        item.setTenantName(props.getStr("tenantName"));

        item.setTableList(props.getStr("tableList"));
        item.setDatabaseName(props.getStr("database-name"));
        item.setTableName(props.getStr("table-name"));

        item.setHostname(props.getStr("hostname"));
        item.setPort(props.getInt("port", 2883));
        item.setConnectTimeout(props.getLong("connect.timeout", 30000L));
        item.setServerTimeZone(props.getStr("server-time-zone"));
        item.setLogProxyHost(props.getStr("logproxy.host"));
        item.setLogproxyPort(props.getInt("logproxy.port", 2983));
        item.setWorkingMode(props.getStr("working-mode", "storage"));
        item.setJdbcDriver(props.getStr("jdbc.driver", "com.mysql.jdbc.Driver"));
        item.setCompatibleMode(props.getStr("compatibleMode", "mysql"));

        if (CharSequenceUtil.isNotBlank(item.getTableList())) {
            //提炼出源的数据库
            String[] split = item.getTableList().split(",");
            Set<String> allDbs = new HashSet<>();
            for (String s : split) {
                int i = s.indexOf(".");
                allDbs.add(s.substring(0, i));
            }
            item.setAllDbs(allDbs);
            if (allDbs.isEmpty()) {
                throw ConfigureException.of("没有从tableList 获取到任何数据库信息");
            }
        } else {
            if (!(CharSequenceUtil.isNotBlank(item.getDatabaseName()) && CharSequenceUtil.isNotBlank(item.getTableName()))) {
                throw ConfigureException.of("tableList 或者 database-name,table-name不能同时为空");
            }

        }
        //目标数据库
        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl(props.getStr("dest.jdbc.url"));
        jdbcInfo.setUserName(props.getStr("dest.jdbc.username"));
        jdbcInfo.setPassword(props.getStr("dest.jdbc.password"));
        jdbcInfo.setDriverClass(props.getStr("dest.jdbc.driver"));
        item.setDestJdbc(jdbcInfo);
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
        return item;
    }

}
