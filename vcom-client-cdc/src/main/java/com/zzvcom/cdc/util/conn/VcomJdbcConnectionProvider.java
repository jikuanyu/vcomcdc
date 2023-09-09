package com.zzvcom.cdc.util.conn;

import com.zzvcom.cdc.cfg.JdbcInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * @author yujikuan
 */
public class VcomJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger log = LoggerFactory.getLogger(VcomJdbcConnectionProvider.class);
    private static final long serialVersionUID = 1L;
    private final JdbcInfo jdbcInfo;
    private transient Driver loadedDriver;
    private transient Connection connection;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }


    public VcomJdbcConnectionProvider(JdbcInfo jdbcInfo) {
        this.jdbcInfo = jdbcInfo;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection != null
                && connection.isValid(jdbcInfo.getConnectionCheckTimeoutSeconds());
    }

    private static Driver loadDriver(String driverName)
            throws SQLException, ClassNotFoundException {

        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        // We could reach here for reasons:
        // * Class loader hell of DriverManager(see JDK-8146872).
        // * driver is not installed as a service provider.
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (Driver) clazz.newInstance();
        } catch (Exception ex) {
            throw new SQLException("Fail to create driver of class " + driverName, ex);
        }
    }

    private Driver getLoadedDriver() throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
            loadedDriver = loadDriver(jdbcInfo.getDriverClass());
        }
        return loadedDriver;
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (connection != null) {
            return connection;
        }
        if (jdbcInfo.getDriverClass() == null) {
            connection =
                    DriverManager.getConnection(
                            jdbcInfo.getUrl(),
                            jdbcInfo.getUserName(),
                            jdbcInfo.getPassword());
        } else {
            Driver driver = getLoadedDriver();
            Properties info = new Properties();
            info.setProperty("user", jdbcInfo.getUserName());
            info.setProperty("password", jdbcInfo.getPassword());
            connection = driver.connect(jdbcInfo.getUrl(), info);
            if (connection == null) {
                // Throw same exception as DriverManager.getConnection when no driver found to match
                // caller expectation.
                throw new SQLException(
                        "No suitable driver found for " + jdbcInfo.getUrl(), "08001");
            }
        }
        return connection;
    }

    @Override
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }

    @Override
    public JdbcInfo getJdbcInfo() {
        return jdbcInfo;
    }
}
