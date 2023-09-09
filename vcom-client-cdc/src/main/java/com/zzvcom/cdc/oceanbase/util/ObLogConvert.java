package com.zzvcom.cdc.oceanbase.util;

import cn.hutool.core.convert.Convert;
import com.zzvcom.cdc.jdbc.JdbcColumn;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * @author yujikuan
 * @Classname ObLogConvert
 * @Description 进行oblog日志的转变
 * @Date 2023/8/23 17:42
 */
public class ObLogConvert {
    private ObLogConvert() {
    }

    /**
     * 进行类型转换
     *
     * @param jdbcColumn
     * @param field
     * @return
     * @throws SQLException
     */
    public static Object convertByLogObj(JdbcColumn jdbcColumn, Object field) {
        if (field == null) {
            return null;
        }
        Object result = null;
        int type = jdbcColumn.getDataType();
        try {
            switch (type) {
                case java.sql.Types.NULL:
                    break;
                case java.sql.Types.BOOLEAN:
                case java.sql.Types.BIT:
                    result = Convert.toBool(field);
                    break;
                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.LONGNVARCHAR:
                    result = field;
                    break;
                case java.sql.Types.TINYINT:
                    result = Convert.toByte(field);
                    break;
                case java.sql.Types.SMALLINT:
                    result = Convert.toShort(field);
                    break;
                case java.sql.Types.INTEGER:
                    result = Convert.toInt(field);
                    break;
                case java.sql.Types.BIGINT:
                    result = Convert.toLong(field);
                    break;
                case java.sql.Types.REAL:
                    result = Convert.toFloat(field);
                    break;
                case java.sql.Types.FLOAT:
                case java.sql.Types.DOUBLE:
                    result = Convert.toDouble(field);
                    break;
                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    result = Convert.toBigDecimal(field);
                    break;
                case java.sql.Types.DATE:
                    if (jdbcColumn.getTypeName().equalsIgnoreCase("year")) {
                        result = Integer.parseInt(field.toString());
                    } else {
                        Date tmpDate = null;
                        java.util.Date date = Convert.toDate(field);
                        tmpDate = new Date(date.getTime());
                        result = tmpDate;
                    }
                    break;
                case java.sql.Types.TIME:
                    break;
                case java.sql.Types.TIMESTAMP:
                    Timestamp ts = null;
                    LocalDateTime localDateTime = Convert.toLocalDateTime(field);
                    ts = Timestamp.valueOf(localDateTime);
                    result = ts;
                    break;
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.LONGVARBINARY:
                    result = Convert.toPrimitiveByteArray(field);
                    break;
                default:

            }
        } catch (ClassCastException e) {
            String errorMessage =
                    String.format(
                            "%s, jdbcColumn: %s, field value: %s.",
                            e.getMessage(), jdbcColumn, field);
            ClassCastException enrichedException = new ClassCastException(errorMessage);
            enrichedException.setStackTrace(e.getStackTrace());
            throw enrichedException;
        }

        return result;
    }
}
