package com.zzvcom.cdc.jdbc;

import cn.hutool.core.collection.CollUtil;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.util.JdbcConnnUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 解析目标库中所有的表信息。
 */
public class JdbcParserMata {

    private static final String TABLE_NAME = "TABLE_NAME";

    private JdbcParserMata() {

    }

    private static final Logger log = LoggerFactory.getLogger(JdbcParserMata.class);

    /**
     * 解析数据库中的表名
     *
     * @param connection 连接,改连接会自动关闭
     * @param dbs        数据列表
     */
    public static List<JdbcTable> parserMata(Connection connection, Set<String> dbs, Collection<String> tables, boolean isClose) throws SQLException {
        List<JdbcTable> list = new ArrayList<>();
        if (CollUtil.isEmpty(dbs)) {
            return list;
        }
        if (connection == null) {
            return list;
        }
        DatabaseMetaData metaData = null;
        try {
            metaData = connection.getMetaData();
            for (String db : dbs) {
                ResultSet rs = metaData.getTables(db, null, "%", null);
                while (rs.next()) {
                    String tableName = rs.getString(TABLE_NAME);
                    String tableType = rs.getString("TABLE_TYPE");
                    log.debug("tableName={},tableType={}", tableName, tableType);
                    if (tables != null && !tables.contains(db + "." + tableName)) {
                        continue;
                    }
                    JdbcTable table = new JdbcTable();
                    table.setDb(db);
                    table.setTableName(tableName);
                    list.add(table);
                    addColumn(db, tableName, metaData, table);
                }
                rs.close();
            }
        } catch (SQLException throwable) {
            log.error("throwable", throwable);
        } finally {
            if (isClose) {
                connection.close();
            }
        }
        return list;
    }


    public static Map<String, JdbcTable> parserMataMap(Connection connection, Set<String> dbs, Collection<String> tables, boolean isClose) throws SQLException {
        return parserMata(connection, dbs, tables, isClose).stream().collect(Collectors.toMap(item -> item.getDb() + "." + item.getTableName(), Function.identity()));
    }


    public static Map<String, JdbcTable> parserMataMapByJdbcInfo(JdbcInfo jdbcInfo, String dbPattern, String tbPattern) throws SQLException, ConfigurationException, ClassNotFoundException {
        Connection conn = JdbcConnnUtil.getConn(jdbcInfo);
        final List<String> tables = getTables(conn, dbPattern, tbPattern, "mysql", false);
        log.info("tables={}", tables);
        final Set<String> dbs = tables.stream().map(item -> {
            final int index = item.indexOf(".");
            return item.substring(0, index);
        }).collect(Collectors.toSet());

        Map<String, JdbcTable> map = parserMataMap(conn, dbs, tables, true);
        for (Map.Entry<String, JdbcTable> entry : map.entrySet()) {
            entry.getValue().fixColumnMap();
        }
        return map;


    }

    public static Map<String, JdbcTable> parserMataMapByJdbcInfo(JdbcInfo jdbcInfo, Set<String> dbs,Collection<String> tables) throws SQLException, ClassNotFoundException {
        Connection conn = JdbcConnnUtil.getConn(jdbcInfo);
        Map<String, JdbcTable> map = parserMataMap(conn, dbs, tables, true);
        for (Map.Entry<String, JdbcTable> entry : map.entrySet()) {
            entry.getValue().fixColumnMap();
        }
        return map;
    }


    public static Map<String, JdbcTable> parserMataMapByJdbcInfo(JdbcInfo jdbcInfo, Set<String> dbs) throws SQLException, ClassNotFoundException {
        Connection conn = JdbcConnnUtil.getConn(jdbcInfo);
        Map<String, JdbcTable> map = parserMataMap(conn, dbs, null, true);
        for (Map.Entry<String, JdbcTable> entry : map.entrySet()) {
            entry.getValue().fixColumnMap();
        }
        return map;
    }

    /**
     * 获取特定 特定库表的数据结构
     *
     * @param connection 连接
     * @param database   数据库名称
     * @param table      表的名称
     * @param isClose    是否要求连接关闭
     * @return
     * @throws SQLException
     */
    public static JdbcTable getTableMata(Connection connection, String database, String table, boolean isClose) throws SQLException {
        if (connection == null) {
            return null;
        }
        JdbcTable jdbcTable = new JdbcTable();
        jdbcTable.setDb(database);
        jdbcTable.setTableName(table);
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            addColumn(database, table, metaData, jdbcTable);
        } finally {
            if (isClose) {
                connection.close();
            }
        }
        return jdbcTable;
    }

    /**
     * 获取特定 特定库表的数据结构
     *
     * @param jdbcInfo jdbc连接信息
     * @param database 数据库
     * @param table    表
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */

    public static JdbcTable getTableMataByJdbcInfo(JdbcInfo jdbcInfo, String database, String table) throws SQLException, ClassNotFoundException {
        Connection connection = JdbcConnnUtil.getConn(jdbcInfo);
        return getTableMata(connection, database, table, true);
    }


    private static void addColumn(String database, String table, DatabaseMetaData metaData, JdbcTable jdbcTable) throws SQLException {
        ResultSet primaryKeys = null;
        Set<String> pks = new HashSet<>();
        try {
            primaryKeys = metaData.getPrimaryKeys(database, null, table);
            while (primaryKeys.next()) {
                String columnName = primaryKeys.getString("COLUMN_NAME");
                pks.add(columnName);
            }
        } finally {
            if (primaryKeys != null) {
                primaryKeys.close();
            }
        }
        ResultSet rsColumn = null;
        try {
            rsColumn = metaData.getColumns(database, null, table, "%");
            while (rsColumn.next()) {

                String columnName = rsColumn.getString("COLUMN_NAME");
                int dataType = rsColumn.getInt("DATA_TYPE");
                final String typeName = rsColumn.getString("TYPE_NAME");
                log.debug("columnName={},dataType={},typeName={}", columnName, dataType, typeName);
                JdbcColumn jdbcColumn = new JdbcColumn();
                jdbcColumn.setTypeName(typeName);
                jdbcColumn.setDataType(dataType);
                jdbcColumn.setName(columnName);
                jdbcColumn.setPK(pks.contains(columnName));
                jdbcTable.addColumn(jdbcColumn);
            }
        } finally {
            if (rsColumn != null) {
                rsColumn.close();
            }
        }
    }


    public static List<String> getTables(Connection connection, String dbPattern, String tbPattern, String compatibleMode, boolean isClose) throws SQLException, ConfigurationException {
        List<String> result = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        switch (compatibleMode) {
            case "mysql":
                List<String> dbNames = getResultList(metaData.getCatalogs(), "TABLE_CAT");
                dbNames =
                        dbNames.stream()
                                .filter(dbName -> Pattern.matches(dbPattern, dbName))
                                .collect(Collectors.toList());
                for (String dbName : dbNames) {
                    List<String> tableNames =
                            getResultList(
                                    metaData.getTables(dbName, null, null, new String[]{"TABLE"}),
                                    TABLE_NAME);
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(dbName + "." + tbName));
                }
                break;
            case "oracle":
                dbNames = getResultList(metaData.getSchemas(), "TABLE_SCHEM");
                dbNames =
                        dbNames.stream()
                                .filter(dbName -> Pattern.matches(dbPattern, dbName))
                                .collect(Collectors.toList());
                for (String dbName : dbNames) {
                    List<String> tableNames =
                            getResultList(
                                    metaData.getTables(null, dbName, null, new String[]{"TABLE"}),
                                    TABLE_NAME);
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(dbName + "." + tbName));
                }
                break;
            default:
                throw new ConfigurationException("Unsupported compatible mode: " + compatibleMode);
        }
        if (isClose) {
            connection.close();
        }
        return result;
    }

    private static List<String> getResultList(ResultSet resultSet, String columnName) throws SQLException {
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString(columnName));
        }
        return result;
    }

}
