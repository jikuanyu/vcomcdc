package com.zzvcom.cdc.cfg;

/**
 * jdbcInfo信息
 */
public class JdbcInfo implements java.io.Serializable {
    private static final long serialVersionUID = 3240349303942092497L;
    private String url;
    private String userName;
    private String password;
    private String driverClass;
    /**
     * 批量插入设置
     */
    private BatchSinkOption batchSinkOption;

    private int connectionCheckTimeoutSeconds =30;
    /**
     * 使用数据库连接池的时候使用"org.mariadb.jdbc.MariaDbDataSource"
     */
    private String dataSourceClassName;


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    public void setConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
    }

    public BatchSinkOption getBatchSinkOption() {
        return batchSinkOption;
    }

    public void setBatchSinkOption(BatchSinkOption batchSinkOption) {
        this.batchSinkOption = batchSinkOption;
    }

    public String getDataSourceClassName() {
        return dataSourceClassName;
    }

    public void setDataSourceClassName(String dataSourceClassName) {
        this.dataSourceClassName = dataSourceClassName;
    }

    @Override
    public String toString() {
        return "JdbcInfo{" +
                "url='" + url + '\'' +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", driverClass='" + driverClass + '\'' +
                ", batchSinkOption=" + batchSinkOption +
                ", connectionCheckTimeoutSeconds=" + connectionCheckTimeoutSeconds +
                ", dataSourceClassName='" + dataSourceClassName + '\'' +
                '}';
    }

}
