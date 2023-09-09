package com.zzvcom.cdc.test;

import cn.hutool.core.collection.CollUtil;
import com.zzvcom.cdc.jdbc.JdbcParserMata;
import com.zzvcom.cdc.jdbc.JdbcTable;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.util.JdbcConnnUtil;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.sql.*;
import java.util.Map;

public class TestJDBC {

    private static final Logger log = LoggerFactory.getLogger(TestJDBC.class);

    @Test
    public void testJDBCMeta() throws SQLException, ClassNotFoundException {

       /* JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://172.18.252.140:3306/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true");
        jdbcInfo.setUserName("vcom");
        jdbcInfo.setPassword("123456");
        jdbcInfo.setDriverClass("com.mysql.jdbc.Driver");
        */


        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://172.18.252.140:2883/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true");
        jdbcInfo.setUserName("root@mq_t1");
        jdbcInfo.setPassword("vcom123456");
        jdbcInfo.setDriverClass("com.mysql.jdbc.Driver");


        Connection conn = JdbcConnnUtil.getConn(jdbcInfo);
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet schemas = metaData.getTables("test_ob_to_mysql", null, null, null);
        println(schemas, metaData);

    }

    private void println(ResultSet rs, DatabaseMetaData metaData) throws SQLException {
        while (rs.next()) {
            String table_name = rs.getString("TABLE_NAME");
            String table_cat = rs.getString("TABLE_CAT");
            log.debug("table={}", table_name);
            log.debug("db={}", table_cat);
            ResultSet columns = metaData.getColumns(
                    table_cat,
                    null,
                    table_name,
                    "%");
            ResultSet primaryKeys = metaData.getPrimaryKeys(table_cat, null, table_name);
            while (primaryKeys.next()) {
                String column_name = primaryKeys.getString("COLUMN_NAME");
                String pk_name = primaryKeys.getString("PK_NAME");
                log.debug("pk_name={}", pk_name);
                log.debug("column_name={}", column_name);

            }
            primaryKeys.close();

            while (columns.next()) {
                String column_name = columns.getString("COLUMN_NAME");
                int data_type = columns.getInt("DATA_TYPE");
                log.debug("column_name={},data_type={}", column_name, data_type);
            }
            columns.close();
        }
        rs.close();

    }


    @Test
    public void testJdbcParserMata() throws SQLException, ClassNotFoundException {
        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://192.168.180.234:2883/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&socketTimeout=60000&connecTimeout=5000");
        jdbcInfo.setUserName("root@mq_t1");
        jdbcInfo.setPassword("vcom123456");
        jdbcInfo.setDriverClass("com.mysql.jdbc.Driver");
        Map<String, JdbcTable> dbTableMap = JdbcParserMata.parserMataMapByJdbcInfo(jdbcInfo, CollUtil.newHashSet("mysql", "test_ob_to_mysql"));
        log.debug("dbTableMap={}", dbTableMap);
    }


    @Test
    public void testJdbcParserMataMysql() throws SQLException, ClassNotFoundException {

        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://172.18.252.140:3306/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true");
        jdbcInfo.setUserName("vcom");
        jdbcInfo.setPassword("123456");
        jdbcInfo.setDriverClass("com.mysql.jdbc.Driver");

        Map<String, JdbcTable> dbTableMap = JdbcParserMata.parserMataMapByJdbcInfo(jdbcInfo, CollUtil.newHashSet("test_ob_to_mysql", "mysql"));
        log.debug("dbTableMap={}", dbTableMap);
    }

    @Test
    public void testJdbcParerMataMysqlTable() throws SQLException, ClassNotFoundException {

        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://localhost:3306/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true");
        jdbcInfo.setUserName("root");
        jdbcInfo.setPassword("123456");
        jdbcInfo.setDriverClass("com.mysql.jdbc.Driver");


        String database = "test";
        String table = "access_log";

        JdbcTable tableMataByJdbcInfo = JdbcParserMata.getTableMataByJdbcInfo(jdbcInfo, database, table);
        tableMataByJdbcInfo.fixColumnMap();


        log.debug("tableMataByJdbcInfo={}", tableMataByJdbcInfo);
    }

    @Test
    public void testSourceAndDestInfo() throws SQLException, ClassNotFoundException {
        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://172.18.252.141:3306/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true&connectTimeout=300000&autoReconnect=true");
        jdbcInfo.setUserName("nrms");
        jdbcInfo.setPassword("vcom123456");
        jdbcInfo.setDriverClass("org.mariadb.jdbc.Driver");


        String database = "nrms";
        String table = "mkey";

        JdbcTable tableMataByJdbcInfo = JdbcParserMata.getTableMataByJdbcInfo(jdbcInfo, database, table);

        jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://172.18.252.140:2881/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&connectTimeout=300000&autoReconnect=true&cacheServerConfiguration=true&rewriteBatchedStatements=true&useServerPrepStmts=true&cachePrepStmts=true");
        jdbcInfo.setUserName("root@mq_t1");
        jdbcInfo.setPassword("vcom123456");
        jdbcInfo.setDriverClass("org.mariadb.jdbc.Driver");

        JdbcTable tableMataByJdbcInfo2 = JdbcParserMata.getTableMataByJdbcInfo(jdbcInfo, database, table);
        log.info("tableMataByJdbcInfo={}", tableMataByJdbcInfo);
        log.info("tableMataByJdbcInfo2={}", tableMataByJdbcInfo2);
    }


    @Test
    public void testPattern() throws SQLException, ConfigurationException, ClassNotFoundException {
        JdbcInfo jdbcInfo = new JdbcInfo();
        jdbcInfo.setUrl("jdbc:mysql://172.18.252.141:3306/?useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true&connectTimeout=300000&autoReconnect=true");
        jdbcInfo.setUserName("nrms");
        jdbcInfo.setPassword("vcom123456");
        jdbcInfo.setDriverClass("org.mariadb.jdbc.Driver");
        Map<String, JdbcTable> tables = JdbcParserMata.parserMataMapByJdbcInfo(jdbcInfo, "^nrms$", "(^t_urc_cat\\w*)");
        log.info("tables={}", tables);

    }
}
