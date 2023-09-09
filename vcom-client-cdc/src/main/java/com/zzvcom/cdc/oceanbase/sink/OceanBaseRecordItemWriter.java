package com.zzvcom.cdc.oceanbase.sink;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import com.oceanbase.oms.logmessage.DataMessage;

import com.zzvcom.cdc.jdbc.JdbcColumn;
import com.zzvcom.cdc.jdbc.JdbcTable;
import com.zzvcom.cdc.util.JdbcConnnUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * 暂不使用,性能不行
 * 使用oceanBase为sourcejdbc写入
 */
public class OceanBaseRecordItemWriter {

    private static final Logger log = LoggerFactory.getLogger(OceanBaseRecordItemWriter.class);

    private OceanBaseRecordItemWriter() {
    }

    /**
     * 进行sql的执行
     *
     * @param item
     *
     * @param conn
     */
    public static void excSql(OceanBaseRecordItem item, Connection conn) throws SQLException {

        JdbcTable jdbcTable = item.getJdbcTable();
        //查询主键
        List<JdbcColumn> pks = jdbcTable.getColumnMap().entrySet().stream().filter(a -> a.getValue().isPK()).map(Map.Entry::getValue).collect(Collectors.toList());
        if (CollUtil.isEmpty(pks)) {
            log.error("没有主键暂不处理,jdbcTable={}", jdbcTable);
            return;
        }
        //全量执行
        if (item.getRecord().isSnapshotRecord()) {
            //判断是否存在
            insert(item, conn, pks);
        } else if (DataMessage.Record.Type.INSERT == item.getRecord().getOpt()) {
            insert(item, conn, pks);
        } else if (DataMessage.Record.Type.UPDATE == item.getRecord().getOpt()) {
            insertOrUpdate(item, conn, pks);
        } else if (DataMessage.Record.Type.REPLACE == item.getRecord().getOpt()) {
            insertOrUpdate(item, conn, pks);
        } else if (DataMessage.Record.Type.DELETE == item.getRecord().getOpt()) {
            String delSql = OceanBaseRecordItemSqlHelper.getDelSql(item, pks);
            log.debug("delSql={}", delSql);
            PreparedStatement pst = null;
            try {
                pst = conn.prepareStatement(delSql);
                int i = 0;
                for (JdbcColumn pk : pks) {
                    Object o = item.getRecord().getLogMessageFieldsBefore().get(pk.getName());
                    setPst(pst, pk, o, i++);
                }
                pst.executeUpdate();
            } finally {
                if (pst != null) {
                    pst.close();
                }
            }
        } else {
            log.error("不处理改类型,item={}", item);
        }
    }

    private static void insert(OceanBaseRecordItem item, Connection conn, List<JdbcColumn> pks) throws SQLException {
        boolean isExist = isExist(item, pks, conn);
        if (!isExist) {
            insert(item, conn);
        }
    }

    private static void insertOrUpdate(OceanBaseRecordItem item, Connection conn, List<JdbcColumn> pks) throws SQLException {
        boolean isExist = isExist(item, pks, conn);
        if (!isExist) {
            insert(item, conn);
        } else {
            update(item, pks, conn);
        }
    }

    private static void update(OceanBaseRecordItem item, List<JdbcColumn> pks, Connection conn) throws SQLException {
        String updateSql = OceanBaseRecordItemSqlHelper.getUpdateSql(item, pks);
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(updateSql);
            int i = 0;
            for (Map.Entry<String, Object> entry : item.getRecord().getLogMessageFieldsAfter().entrySet()) {
                JdbcColumn jdbcColumn = item.getJdbcTable().getColumnMap().get(entry.getKey());
                if (jdbcColumn == null) {
                    log.error("entry.getKey()={}，找不到对应的行,jdbcTable={},忽略不处理", entry.getKey(), item.getJdbcTable());
                    return;
                }
                setPst(pst, jdbcColumn, entry.getValue(), i++);
            }

            for (JdbcColumn pk : pks) {
                Object o = item.getRecord().getLogMessageFieldsBefore().get(pk.getName());
                setPst(pst, pk, o, i++);
            }
            pst.executeUpdate();
        } finally {
            if (pst != null) {
                pst.close();
            }
        }

    }

    private static void insert(OceanBaseRecordItem item, Connection conn) throws SQLException {
        String insertSql = OceanBaseRecordItemSqlHelper.getInsertSql(item);
        log.debug("insertSql={}", insertSql);
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(insertSql);
            if (item.getRecord().isSnapshotRecord()) {
                int i = 0;
                for (Map.Entry<String, Object> entry : item.getRecord().getJdbcFields().entrySet()) {
                    JdbcColumn jdbcColumn = item.getJdbcTable().getColumnMap().get(entry.getKey());
                    if (jdbcColumn == null) {
                        log.error("isSnapshotRecord entry.getKey()={}，找不到对应的行,jdbcTable={},忽略不处理", entry.getKey(), item.getJdbcTable());
                        return;
                    }
                    JdbcConnnUtil.setField(pst, jdbcColumn, entry.getValue(), i++);
                }
            } else {
                int i = 0;
                for (Map.Entry<String, Object> entry : item.getRecord().getLogMessageFieldsAfter().entrySet()) {
                    JdbcColumn jdbcColumn = item.getJdbcTable().getColumnMap().get(entry.getKey());
                    if (jdbcColumn == null) {
                        log.error("LogMessageFieldsAfter entry.getKey()={}，找不到对应的行,jdbcTable={},忽略不处理", entry.getKey(), item.getJdbcTable());
                        return;
                    }
                    setPst(pst, jdbcColumn, entry.getValue(), i++);
                }
            }
            pst.executeUpdate();
        } finally {
            if (pst != null) {
                pst.close();
            }
        }


    }

    private static boolean isExist(OceanBaseRecordItem item, List<JdbcColumn> pks, Connection conn) throws SQLException {

        String selectSql = OceanBaseRecordItemSqlHelper.getSelectSql(item, pks);
        log.debug("selectSql={}", selectSql);
        boolean flag = false;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(selectSql);
            if (item.getRecord().isSnapshotRecord()) {
                int i = 0;
                for (JdbcColumn pk : pks) {
                    Object o = item.getRecord().getJdbcFields().get(pk.getName());
                    JdbcConnnUtil.setField(preparedStatement, pk, o, i++);
                }
            } else {
                int i = 0;
                Map<String, Object> logMessageMap = null;
                if (DataMessage.Record.Type.UPDATE == item.getRecord().getOpt()) {
                    logMessageMap = item.getRecord().getLogMessageFieldsBefore();
                } else {
                    logMessageMap = item.getRecord().getLogMessageFieldsAfter();
                }
                for (JdbcColumn pk : pks) {
                    Object o = logMessageMap.get(pk.getName());
                    setPst(preparedStatement, pk, o, i++);
                }
            }
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                flag = true;
            }
            resultSet.close();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
        return flag;
    }

    private static void setPst(PreparedStatement upload, JdbcColumn jdbcColumn, Object field, int index) throws SQLException {

        int type = jdbcColumn.getDataType();
        if (field == null) {
            upload.setNull(index + 1, type);
        } else {
            try {
                switch (type) {
                    case java.sql.Types.NULL:
                        upload.setNull(index + 1, type);
                        break;
                    case java.sql.Types.BOOLEAN:
                    case java.sql.Types.BIT:
                        upload.setBoolean(index + 1, Convert.toBool(field));

                        break;
                    case java.sql.Types.CHAR:
                    case java.sql.Types.NCHAR:
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.LONGVARCHAR:
                    case java.sql.Types.LONGNVARCHAR:
                        upload.setString(index + 1, (String) field);
                        break;
                    case java.sql.Types.TINYINT:

                        upload.setByte(index + 1, Convert.toByte(field));
                        break;
                    case java.sql.Types.SMALLINT:

                        upload.setShort(index + 1, Convert.toShort(field));
                        break;
                    case java.sql.Types.INTEGER:

                        upload.setInt(index + 1, Convert.toInt(field));
                        break;
                    case java.sql.Types.BIGINT:
                        upload.setLong(index + 1, Convert.toLong(field));
                        break;
                    case java.sql.Types.REAL:

                        upload.setFloat(index + 1, Convert.toFloat(field));
                        break;
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.DOUBLE:

                        upload.setDouble(index + 1, Convert.toDouble(field));
                        break;
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:

                        upload.setBigDecimal(index + 1, Convert.toBigDecimal(field));
                        break;
                    case java.sql.Types.DATE:

                        Date tmpDate = null;

                            java.util.Date date = Convert.toDate(field);
                            tmpDate = new Date(date.getTime());

                        upload.setDate(index + 1, tmpDate);
                        break;
                    case java.sql.Types.TIME:
                        upload.setTime(index + 1, null);
                        break;
                    case java.sql.Types.TIMESTAMP:
                        Timestamp ts = null;

                            LocalDateTime localDateTime = Convert.toLocalDateTime(field);
                            ts = Timestamp.valueOf(localDateTime);

                        upload.setTimestamp(index + 1, ts);
                        break;
                    case java.sql.Types.BINARY:
                    case java.sql.Types.VARBINARY:
                    case java.sql.Types.LONGVARBINARY:
                        upload.setBytes(index + 1, Convert.toPrimitiveByteArray(field));
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


    }
}
