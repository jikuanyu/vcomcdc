package com.zzvcom.cdc.mysql.cfg;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.zzvcom.cdc.cfg.JdbcInfo;

/**
 * @author yujikuan
 */
public class MysqlToJdbc implements java.io.Serializable {

    private static final long serialVersionUID = -3617395018430805571L;
    /**
     * MySQL 数据库服务器的 IP 地址或主机名。
     * */
    private String hostname;
    /**
     * MySQL 数据库服务器的整数端口号。
     * */
    private int port;

    /**
     *  连接到 MySQL 数据库服务器时要使用的 MySQL 用户的名称。
     */
    private String username;

    /**连接 MySQL 数据库服务器时使用的密码。
     *
     */
    private String password;

    /* 要监视的 MySQL 服务器的数据库名称。数据库名称还支持正则表达式，以监视多个与正则表达式匹配的表。
       database-name=mysqltoob
     */
    private String databaseName;
    /**
     * 需要监视的 MySQL 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。注意：MySQL CDC 连接器在正则匹配表名时，会把用户填写的 database-name， table-name 通过字符串 `\\.` 连接成一个全路径的正则表达式，然后使用该正则表达式和 MySQL 数据库中表的全限定名进行正则匹配。
     */
    private String tableName;

    /**
     * server-id=
     */
    private String serverId;

    /*读取数据使用的 server id，server id 可以是个整数或者一个整数范围，比如 '5400' 或 '5400-5408',
    建议在 'scan.incremental.snapshot.enabled' 参数为启用时，配置成整数范围。因为在当前 MySQL 集群中运行的所有 slave 节点，
    标记每个 salve 节点的 id 都必须是唯一的。 所以当连接器加入 MySQL 集群作为另一个 slave 节点（并且具有唯一 id 的情况下），
    它就可以读取 binlog。 默认情况下，连接器会在 5400 和 6400 之间生成一个随机数，但是我们建议用户明确指定 Server id。
    scan.incremental.snapshot.enabled=true
     */
    private Boolean scanIncrementalSnapshot = true;


    /**
     * scan.incremental.snapshot.chunk.size
     */
    private int splitSize;
    /**
     * scan.incremental.snapshot.chunk.size
     */
    private int fetchSize;
    /**
     * 连接器在尝试连接到 MySQL 数据库服务器后超时前应等待的最长时间。
     */
    private Long connectTimeout;

    /**
     * 启动模式
     */
    private StartupOptions startupOptions;

    public StartupOptions getStartupOptions() {
        return startupOptions;
    }

    public void setStartupOptions(StartupOptions startupOptions) {
        this.startupOptions = startupOptions;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * 目标表的jdbcinfo
     * jdbcinfo
     */
    private JdbcInfo jdbcInfo;


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

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Boolean getScanIncrementalSnapshot() {
        return scanIncrementalSnapshot;
    }

    public void setScanIncrementalSnapshot(Boolean scanIncrementalSnapshot) {
        this.scanIncrementalSnapshot = scanIncrementalSnapshot;
    }

    public JdbcInfo getJdbcInfo() {
        return jdbcInfo;
    }

    public void setJdbcInfo(JdbcInfo jdbcInfo) {
        this.jdbcInfo = jdbcInfo;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    @Override
    public String toString() {
        return "MysqlToJdbc{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", serverId='" + serverId + '\'' +
                ", scanIncrementalSnapshot=" + scanIncrementalSnapshot +
                ", splitSize=" + splitSize +
                ", fetchSize=" + fetchSize +
                ", connectTimeout=" + connectTimeout +
                ", startupOptions=" + startupOptions +
                ", jdbcInfo=" + jdbcInfo +
                '}';
    }
}
