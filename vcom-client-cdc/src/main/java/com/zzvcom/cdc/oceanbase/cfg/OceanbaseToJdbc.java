package com.zzvcom.cdc.oceanbase.cfg;


import com.zzvcom.cdc.cfg.JdbcInfo;

import java.util.Set;

/**
 * @author yujikuan
 */
public class OceanbaseToJdbc implements java.io.Serializable {


    private static final long serialVersionUID = 1152428216311419027L;
    /**
     * # OceanBase root 服务器列表，服务器格式为 `ip:rpc_port:sql_port`，多个服务器地址使用英文分号 `;` 隔开，OceanBase 社区版本必填。
     * rootserver-list=192.168.180.231:2882:2881;192.168.180.232:2882:2881;192.168.180.233:2882:2881
     */

    private String rootserverList;

    /**
     * initial latest-offset timestamp
     */

    private String startupMode;
    /**
     * startupMode 为timestamp的时候有效
     */
    private Long startupTimestamp;

    /**
     * OceanBase 数据库或 OceanBbase 代理 ODP 的 IP 地址或主机名。
     */
    private String hostname;

    /**
     * OceanBase 数据库服务器的整数端口号。可以是 OceanBase 服务器的 SQL 端口号（默认值为 2881）
     * 或 OceanBase代理服务的端口号（默认值为 2883）
     */
    private int port;
    /**
     * 连接 OceanBase 数据库的用户的名称。
     */
    private String username;

    /**
     * 连接 OceanBase 数据库时使用的密码。
     */
    private String password;
    /**
     * 租户待监控 OceanBase 数据库的租户名，应该填入精确值。
     */
    private String tenantName;
    /**
     * 待监控 OceanBase
     * 数据库的全路径的表名列表，逗号分隔，如："db1.table1, db2.table2"。
     * tableList=test_ob_to_mysql.tbl1,test_ob_to_mysql.tbl2,test_ob_to_mysql.tb3
     */
    private String tableList;

    /**
     * database-name  待监控 OceanBase 数据库的数据库名，应该是正则表达式，该选项只支持和 'initial' 模式一起使用。
     */
    private String databaseName;

     /**
     *  table-name 待监控 OceanBase 数据库的数据库名，应该是正则表达式，该选项只支持和 'initial' 模式一起使用
     */
     private String tableName;

    /**
     * 连接器在尝试连接到 OceanBase 数据库服务器超时前的最长时间。
     */

    private String compatibleMode;

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public void setCompatibleMode(String compatibleMode) {
        this.compatibleMode = compatibleMode;
    }

    private long connectTimeout;


    /**
     * erver-time-zone=+08:00
     * 数据库服务器中的会话时区，用户控制 OceanBase 的时间类型如何转换为 STRING。
     * 合法的值可以是格式为"±hh:mm"的 UTC 时区偏移量，
     * 如果 mysql 数据库中的时区信息表已创建，合法的值则可以是创建的时区。
     */
    private String serverTimeZone;

    /**
     * OceanBase 日志代理服务 的 IP 地址或主机名。
     * logproxy.host=192.168.180.234
     */
    private String logProxyHost;

    /*
    OceanBase 日志代理服务的端口号。
    logproxy.port=2983
    日志代理中 `libobcdc`的工作模式 ,可以是 `storage`或 `memory`。*/

    private int logproxyPort;

    /**
     * working-mode
     * <p>
     * 日志代理中 `libobcdc` 的工作模式 , 可以是 `storage` 或 `memory`。
     */

    private String workingMode;

    /**
     * jdbc.driver=com.mysql.jdbc.Driver
     */

    private String jdbcDriver;




    /**
     * 目的数据源信息
     */
    private JdbcInfo destJdbc;

    public String getStartupMode() {
        return startupMode;
    }

    public void setStartupMode(String startupMode) {
        this.startupMode = startupMode;
    }



    public String getRootserverList() {
        return rootserverList;
    }

    public void setRootserverList(String rootserverList) {
        this.rootserverList = rootserverList;
    }



    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getTableList() {
        return tableList;
    }

    public void setTableList(String tableList) {
        this.tableList = tableList;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public void setServerTimeZone(String serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
    }

    public String getLogProxyHost() {
        return logProxyHost;
    }

    public void setLogProxyHost(String logProxyHost) {
        this.logProxyHost = logProxyHost;
    }

    public int getLogproxyPort() {
        return logproxyPort;
    }

    public void setLogproxyPort(int logproxyPort) {
        this.logproxyPort = logproxyPort;
    }

    public String getWorkingMode() {
        return workingMode;
    }

    public void setWorkingMode(String workingMode) {
        this.workingMode = workingMode;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public JdbcInfo getDestJdbc() {
        return destJdbc;
    }

    public void setDestJdbc(JdbcInfo destJdbc) {
        this.destJdbc = destJdbc;
    }

    private Set<String> allDbs;

    public Set<String> getAllDbs() {
        return allDbs;
    }

    public void setAllDbs(Set<String> allDbs) {
        this.allDbs = allDbs;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getStartupTimestamp() {
        return startupTimestamp;
    }

    public void setStartupTimestamp(Long startupTimestamp) {
        this.startupTimestamp = startupTimestamp;
    }

    @Override
    public String toString() {
        return "OceanbaseToJdbc{" +
                "rootserverList='" + rootserverList + '\'' +
                ", startupMode='" + startupMode + '\'' +
                ", startupTimestamp=" + startupTimestamp +
                ", hostname='" + hostname + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", tenantName='" + tenantName + '\'' +
                ", tableList='" + tableList + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", compatibleMode='" + compatibleMode + '\'' +
                ", connectTimeout=" + connectTimeout +
                ", serverTimeZone='" + serverTimeZone + '\'' +
                ", logProxyHost='" + logProxyHost + '\'' +
                ", logproxyPort=" + logproxyPort +
                ", workingMode='" + workingMode + '\'' +
                ", jdbcDriver='" + jdbcDriver + '\'' +
                ", destJdbc=" + destJdbc +
                ", allDbs=" + allDbs +
                '}';
    }
}
