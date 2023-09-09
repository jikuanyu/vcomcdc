package com.zzvcom.cdc.util;

import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.jdbc.JdbcColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 连接数据库帮助类
 *
 * @author yujikuan
 */
public class JdbcConnnUtil {

    private static final Logger log = LoggerFactory.getLogger(JdbcConnnUtil.class);

    private JdbcConnnUtil() {
    }

    public static Connection getConn(JdbcInfo info) throws SQLException, ClassNotFoundException {
        Class.forName(info.getDriverClass());
        //获取连接
        return DriverManager.getConnection(info.getUrl(), info.getUserName(), info.getPassword());
    }


    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException throwables) {
                //可以忽略异常
                log.error("close...", throwables);
            }
        }
    }

    public static void setField(PreparedStatement upload, JdbcColumn column, Object field, int index)
            throws SQLException {
        int type = column.getDataType();
        if (field == null) {
            upload.setNull(index + 1, type);
            return;
        }
        try {
            // casting values as suggested by
            // http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
            switch (type) {
                case java.sql.Types.NULL:
                    upload.setNull(index + 1, type);
                    break;
                case java.sql.Types.BOOLEAN:
                case java.sql.Types.BIT:
                    upload.setBoolean(index + 1, (boolean) field);
                    break;
                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.LONGNVARCHAR:
                    upload.setString(index + 1, (String) field);
                    break;
                case java.sql.Types.TINYINT:
                    if (field instanceof Integer) {
                        upload.setInt(index + 1, (int) field);
                    } else if (field instanceof Byte) {
                        upload.setByte(index + 1, (byte) field);
                    }
                    break;
                case java.sql.Types.SMALLINT:
                    if (field instanceof Integer) {
                        upload.setShort(index + 1, ((Integer) field).shortValue());
                    } else {
                        upload.setShort(index + 1, (short) field);
                    }
                    break;
                case java.sql.Types.INTEGER:
                    upload.setInt(index + 1, (int) field);
                    break;
                case java.sql.Types.BIGINT:
                    upload.setLong(index + 1, (long) field);
                    break;
                case java.sql.Types.REAL:
                    upload.setFloat(index + 1, (float) field);
                    break;
                case java.sql.Types.FLOAT:
                case java.sql.Types.DOUBLE:
                    upload.setDouble(index + 1, (double) field);
                    break;
                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    upload.setBigDecimal(index + 1, (java.math.BigDecimal) field);
                    break;
                case java.sql.Types.DATE:
                    pstDate(upload, column, field, index);
                    break;
                case java.sql.Types.TIME:
                    upload.setTime(index + 1, (java.sql.Time) field);
                    break;
                case java.sql.Types.TIMESTAMP:
                    if (field instanceof java.time.LocalDateTime) {
                        upload.setTimestamp(index + 1, java.sql.Timestamp.valueOf((java.time.LocalDateTime) field));
                    } else if (field instanceof java.sql.Timestamp) {
                        upload.setTimestamp(index + 1, (java.sql.Timestamp) field);
                    } else {
                        throw FatalException.of("未知错误 field=" + field + ",classname=" + field.getClass());
                    }
                    break;
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.LONGVARBINARY:
                    upload.setBytes(index + 1, (byte[]) field);
                    break;
                default:
                    upload.setObject(index + 1, field);
                    log.warn(
                            "Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
                            type,
                            index + 1,
                            field);
                    // case java.sql.Types.SQLXML
                    // case java.sql.Types.ARRAY:
                    // case java.sql.Types.JAVA_OBJECT:
                    // case java.sql.Types.BLOB:
                    // case java.sql.Types.CLOB:
                    // case java.sql.Types.NCLOB:
                    // case java.sql.Types.DATALINK:
                    // case java.sql.Types.DISTINCT:
                    // case java.sql.Types.OTHER:
                    // case java.sql.Types.REF:
                    // case java.sql.Types.ROWID:
                    // case java.sql.Types.STRUC
            }
        } catch (ClassCastException e) {
            // enrich the exception with detailed information.
            String errorMessage =
                    String.format(
                            "%s, field index: %s, field value: %s.",
                            e.getMessage(), index, field);
            ClassCastException enrichedException = new ClassCastException(errorMessage);
            enrichedException.setStackTrace(e.getStackTrace());
            throw enrichedException;
        }
    }

    private static void pstDate(PreparedStatement upload, JdbcColumn column, Object field, int index) throws SQLException {
        if (column.getTypeName().equalsIgnoreCase("year")) {
            int year = 0;
            if (field instanceof java.sql.Date) {
                year = ((java.sql.Date) field).toLocalDate().getYear();
            } else if (field instanceof Integer) {
                year = (Integer) field;
            }else{
                log.error("pstDate year ,column={},field={}",column,field);
                throw FatalException.of("pstDate year"+field);
            }
            upload.setInt(index + 1, year);
        } else {
            upload.setDate(index + 1, (java.sql.Date) field);
        }
    }
}
