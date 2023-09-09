package com.zzvcom.cdc.mysql.simple;

import cn.hutool.core.convert.Convert;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.zzvcom.cdc.ex.FatalException;
import io.debezium.data.Envelope;
import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TableItemSqlHelper {

    private static final Logger log = LoggerFactory.getLogger(TableItemSqlHelper.class);
    private static final String WHERE = " where ";
    private static final String AND = " and ";

    private TableItemSqlHelper() {
    }

    //"delete from table where (id=1 and name=2) or "
    public static String getDeleteSql(TableItem tableItem) {
        String sql = "delete from " + tableItem.getDb() + "." + tableItem.getTable() + WHERE;
        if (tableItem.getPks() != null) {
            return sql + tableItem.getPks().stream().map(item -> (item.getName() + "=?")).collect(Collectors.joining(AND));
        }
        //暂不支持没有主键的表删除和更新
        return null;
    }

    public static String getBatchDeleteSql(List<TableItem> list) {
        TableItem tableItem = list.get(0);
        String sql = "delete from " + tableItem.getDb() + "." + tableItem.getTable() + WHERE;
        if (tableItem.getPks().size() > 1) {
            throw FatalException.of("批量删除不支持，多个主键的情况");
        }
        if (tableItem.getPks() == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(sql);
        final ColumnItem pkColumn = tableItem.getPks().get(0);
        sb.append(pkColumn.getName());
        sb.append(" in (");
        if (Schema.Type.STRING == pkColumn.getType()) {
            sb.append(list.stream().map(a -> "'" + a.getPks().get(0).getValue() + "'").collect(Collectors.joining(",")));
        } else {
            sb.append(list.stream().map(a -> a.getPks().get(0).getValue().toString()).collect(Collectors.joining(",")));
        }
        sb.append(")");
        return sb.toString();
    }


    //update table  set a=?,b=? where id=? and id2=?
    public static String getUpdateSql(TableItem tableItem) {

        String sql = "update " + tableItem.getDb() + "." + tableItem.getTable();
        String setSql = " set " + tableItem.getAfter().stream().map(item -> item.getName() + "=?").collect(Collectors.joining(","));
        if (tableItem.getPks() != null) {
            return sql + setSql + WHERE + tableItem.getPks().stream().map(item -> (item.getName() + "=?")).collect(Collectors.joining(AND));
        }
        return null;
    }

    //insert table (id,name) values(?,?)
    public static String getInsertSql(TableItem tableItem) {
        String sql = "insert into " + tableItem.getDb() + "." + tableItem.getTable();
        String nameSql = tableItem.getAfter().stream().map(ColumnItem::getName).collect(Collectors.joining(","));
        String endSql = tableItem.getAfter().stream().map(item -> "?").collect(Collectors.joining(","));
        return sql + " (" + nameSql + ")" + "values(" + endSql + ")";
    }

    public static String getReplaceInsertSql(TableItem tableItem) {
        String sql = "replace  into " + tableItem.getDb() + "." + tableItem.getTable();
        String nameSql = tableItem.getAfter().stream().map(ColumnItem::getName).collect(Collectors.joining(","));
        String endSql = tableItem.getAfter().stream().map(item -> "?").collect(Collectors.joining(","));
        return sql + " (" + nameSql + ")" + "values(" + endSql + ")";
    }

    /**
     * insert into test_replace (c1, c2, c3) values (?, ?, ？) on duplicate key update c1=values(c1), c2=values(c2), c3=values(c3);
     */
    public static String insertOrUpdateSql(TableItem tableItem) {
        final String insertSql = getInsertSql(tableItem);
        final String middleSql = " on duplicate key update ";
        String endSql = tableItem.getAfter().stream().map(item -> item.getName() + "=values(" + item.getName() + ")").collect(Collectors.joining(","));
        return insertSql + middleSql + endSql;
    }

    //查询语句
    public static String getFindSql(TableItem tableItem) {
        if (tableItem.getPks() == null) {
            return null;
        }
        String sql = "select 1 from " + tableItem.getDb() + "." + tableItem.getTable() + WHERE;
        String and = tableItem.getPks().stream().map(item -> item.getName() + "=?").collect(Collectors.joining(AND));
        return sql + and;
    }


    public static void getExcSql(TableItem tableItem, Connection connection) throws SQLException {
        log.debug("tableItem={}", tableItem);
        if (Envelope.Operation.CREATE == tableItem.getOp()) {
            insert(tableItem, connection);
        } else if (Envelope.Operation.READ == tableItem.getOp()) {
            log.debug("READ={}", tableItem);
            String findSql = getFindSql(tableItem);
            log.debug("findSql={}", findSql);
            if (findSql != null) {
                if (isExist(tableItem, connection, findSql)) {
                    update(tableItem, connection);
                } else {
                    insert(tableItem, connection);
                }
            } else {
                insert(tableItem, connection);
            }
        } else if (Envelope.Operation.UPDATE == tableItem.getOp()) {
            update(tableItem, connection);
        } else if (Envelope.Operation.DELETE == tableItem.getOp()) {
            String deleteSql = getDeleteSql(tableItem);
            log.debug("deleteSql={}", deleteSql);
            if (deleteSql == null) {
                return;
            }
            PreparedStatement pst = connection.prepareStatement(deleteSql);
            pstPK(tableItem, pst);
            pst.executeUpdate();
            pst.close();
        } else if (Envelope.Operation.TRUNCATE == tableItem.getOp()) {
            log.debug("TRUNCATE 暂不处理 ={}", tableItem);
        } else {
            log.debug("MESSAGE={}", tableItem);
        }
    }

    public static void pstPK(TableItem tableItem, PreparedStatement pst) throws SQLException {
        int i = 1;
        for (ColumnItem pk : tableItem.getPks()) {
            setPst(pst, i, pk);
            i++;
        }
    }

    private static void update(TableItem tableItem, Connection connection) throws SQLException {
        //存在更新
        String updateSql = getUpdateSql(tableItem);
        if (updateSql == null) {
            return;
        }
        log.debug("updateSql={}", updateSql);
        PreparedStatement pst = connection.prepareStatement(updateSql);
        pstUpdate(tableItem, pst);
        pst.executeUpdate();
        pst.close();
    }

    public static void pstUpdate(TableItem tableItem, PreparedStatement pst) throws SQLException {
        int i = 1;
        for (ColumnItem columnItem : tableItem.getAfter()) {
            setPst(pst, i, columnItem);
            i++;
        }
        for (ColumnItem pk : tableItem.getPks()) {
            setPst(pst, i, pk);
            i++;
        }
    }


    private static boolean isExist(TableItem tableItem, Connection connection, String findSql) {
        boolean flag = false;
        try (PreparedStatement pst = connection.prepareStatement(findSql)) {
            pstPK(tableItem, pst);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    flag = true;
                }
            }
        } catch (Exception throwables) {
            log.error("isExist ex", throwables);
        }
        return flag;
    }


    private static void insert(TableItem tableItem, Connection connection) throws SQLException {
        String insertSql = getInsertSql(tableItem);
        log.debug("insertSql={}", insertSql);
        PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
        pstInsert(tableItem, preparedStatement);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    public static int pstInsert(TableItem tableItem, PreparedStatement preparedStatement) throws SQLException {
        int i = 1;
        for (ColumnItem columnItem : tableItem.getAfter()) {
            setPst(preparedStatement, i, columnItem);
            i++;
        }
        return i;
    }


    public static void setPst(PreparedStatement preparedStatement, int i, ColumnItem columnItem) throws SQLException {
        log.debug("i={}", i);
        log.debug("(columnItem.getSchemaName()={}", columnItem);
        if (Timestamp.SCHEMA_NAME.equals(columnItem.getSchemaName())) {
            if (columnItem.getValue() != null) {
                Instant instant = Instant.ofEpochMilli((Long) columnItem.getValue());
                ZoneId zone = ZoneId.from(ZoneOffset.UTC);
                LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
                log.debug("localDateTime={}", localDateTime);
                preparedStatement.setTimestamp(i, java.sql.Timestamp.valueOf(localDateTime));

            } else {
                preparedStatement.setTimestamp(i, null);
            }
            return;
        }
        if (ZonedTimestamp.SCHEMA_NAME.equals(columnItem.getSchemaName())) {
            if (columnItem.getValue() != null) {
                LocalDateTime localDateTime = LocalDateTime.parse((String) columnItem.getValue(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                localDateTime = localDateTime.plusHours(8);
                log.debug("localDateTime={}", localDateTime);
                preparedStatement.setTimestamp(i, java.sql.Timestamp.valueOf(localDateTime));
            } else {
                preparedStatement.setTimestamp(i, null);
            }
            return;
        }
        if (Date.SCHEMA_NAME.equals(columnItem.getSchemaName())) {
            if (columnItem.getValue() != null) {
                long epochMillis = TimeUnit.DAYS.toMillis((Integer) columnItem.getValue());
                preparedStatement.setDate(i, new java.sql.Date(epochMillis));
            } else {
                preparedStatement.setDate(i, null);
            }
            return;
        }
        if (Decimal.LOGICAL_NAME.equals(columnItem.getSchemaName())) {
            if (columnItem.getValue() != null && Schema.Type.BYTES == columnItem.getType()) {
                preparedStatement.setBigDecimal(i, (BigDecimal) columnItem.getValue());
            } else {
                preparedStatement.setBigDecimal(i, null);
            }
            return;
        }
        simpleTypePst( preparedStatement,  i,  columnItem);

    }
    private static  void simpleTypePst(PreparedStatement preparedStatement, int i, ColumnItem columnItem) throws SQLException {

        if (Schema.Type.INT8 == columnItem.getType()) {
            if (columnItem.getValue() == null) {
                preparedStatement.setNull(i, java.sql.Types.TINYINT);
            } else {
                preparedStatement.setByte(i, Convert.toByte(columnItem.getValue()));
            }
        } else if (Schema.Type.INT16 == columnItem.getType()) {
            if (columnItem.getValue() == null) {
                preparedStatement.setNull(i, java.sql.Types.SMALLINT);
            } else {
                preparedStatement.setShort(i, Convert.toShort(columnItem.getValue()));
            }

        } else if (Schema.Type.INT32 == columnItem.getType()) {
            if (columnItem.getValue() == null) {
                preparedStatement.setNull(i, java.sql.Types.INTEGER);
            } else {
                preparedStatement.setInt(i, Convert.toInt(columnItem.getValue()));
            }
        } else if (Schema.Type.INT64 == columnItem.getType()) {
            if (columnItem.getValue() == null) {
                preparedStatement.setNull(i, java.sql.Types.BIGINT);
            } else {
                preparedStatement.setLong(i, Convert.toLong(columnItem.getValue()));
            }
        } else if (Schema.Type.FLOAT32 == columnItem.getType()) {
            if (columnItem.getValue() == null) {
                preparedStatement.setNull(i, Types.FLOAT);
            } else {
                preparedStatement.setFloat(i, Convert.toFloat(columnItem.getValue()));
            }
        } else if (Schema.Type.FLOAT64 == columnItem.getType()) {
            if (columnItem.getValue() == null) {
                preparedStatement.setNull(i, Types.DOUBLE);
            } else {
                preparedStatement.setDouble(i, Convert.toDouble(columnItem.getValue()));
            }
        } else if (Schema.Type.BOOLEAN == columnItem.getType()) {
            if (columnItem.getValue() == null) {
                preparedStatement.setNull(i, Types.BOOLEAN);
            } else {
                preparedStatement.setBoolean(i, Convert.toBool(columnItem.getValue()));
            }
        } else if (Schema.Type.STRING == columnItem.getType()) {
            preparedStatement.setString(i, Convert.toStr(columnItem.getValue()));
        } else if (Schema.Type.BYTES == columnItem.getType()) {
            preparedStatement.setNull(i, Types.LONGVARBINARY);
        } else {
            log.warn("未知，columnItem={}", columnItem);
            preparedStatement.setObject(i, null);
        }
    }
}
