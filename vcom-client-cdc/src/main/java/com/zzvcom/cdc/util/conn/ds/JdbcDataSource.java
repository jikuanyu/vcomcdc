package com.zzvcom.cdc.util.conn.ds;

import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariConfig;
import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariDataSource;
import com.zzvcom.cdc.cfg.JdbcInfo;

/**
 * @author yujikuan
 * @Classname JdbcDataSource
 * @Description 数据源方式，以便实现多线程数据库同步。
 * @Date 2023/8/17 14:07
 */
public class JdbcDataSource {

    private JdbcDataSource() {
        throw new IllegalStateException("Utility class");
    }

    public static HikariDataSource getDataSource(JdbcInfo jdbcInfo) {
        //https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
        //dataSource.cachePrepStmts=true
        //dataSource.prepStmtCacheSize=250
        //dataSource.prepStmtCacheSqlLimit=2048
        //dataSource.useServerPrepStmts=true
        //dataSource.useLocalSessionState=true
        //dataSource.rewriteBatchedStatements=true
        //dataSource.cacheResultSetMetadata=true
        //dataSource.cacheServerConfiguration=true
        //dataSource.elideSetAutoCommits=true
        //dataSource.maintainTimeStats=false
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcInfo.getUrl());
        config.setUsername(jdbcInfo.getUserName());
        config.setPassword(jdbcInfo.getPassword());
        config.setDriverClassName(jdbcInfo.getDriverClass());
        config.setDataSourceClassName(jdbcInfo.getDataSourceClassName());
        //推荐的配置
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(1);
        config.setConnectionTestQuery("select 1");
        return new HikariDataSource(config);
    }

}
