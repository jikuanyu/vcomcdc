package com.zzvcom.cdc.oceanbase.sink;

import com.zzvcom.cdc.jdbc.JdbcColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OceanBaseRecordItemSqlHelper {

    private static final String AND = " and ";
    private static final String WHERE = " where ";

    private OceanBaseRecordItemSqlHelper() {
    }

    /**
     * OceanBaseRecordItem 组装insert sql
     * 根据
     *
     * @param item
     * @return
     */
    public static String getInsertSql(OceanBaseRecordItem item) {
        String sql = String.format("insert into %s.%s (", item.getRecord().getSourceInfo().getDatabase(), item.getRecord().getSourceInfo().getTable());
        StringBuilder sb = new StringBuilder(sql);

        List<String> columns = new ArrayList<>();
        List<String> marks = new ArrayList<>();
        if (item.getRecord().isSnapshotRecord()) {
            for (Map.Entry<String, Object> entry : item.getRecord().getJdbcFields().entrySet()) {
                columns.add(entry.getKey());
                marks.add("?");
            }
        } else {
            Map<String, Object> logMessageFieldsAfter = item.getRecord().getLogMessageFieldsAfter();
            for (Map.Entry<String, Object> entry : logMessageFieldsAfter.entrySet()) {
                columns.add(entry.getKey());
                marks.add("?");
            }
        }
        sb.append(columns.stream().collect(Collectors.joining(",")));
        sb.append(")values(");
        sb.append(marks.stream().collect(Collectors.joining(",")));
        sb.append(")");
        return sb.toString();
    }

    /**
     * 根据OceanBaseRecordItem和pks组装删除语句
     *
     * @param item
     * @param pks
     * @return
     */
    public static String getSelectSql(OceanBaseRecordItem item, List<JdbcColumn> pks) {
        String sql = String.format("select 1 from  %s.%s", item.getRecord().getSourceInfo().getDatabase(), item.getRecord().getSourceInfo().getTable());
        StringBuilder sb = new StringBuilder(sql);
        sb.append(WHERE);
        sb.append(pks.stream().map(pk -> pk.getName() + "=?").collect(Collectors.joining(AND)));
        return sb.toString();
    }

    /**
     * 根据OceanBaseRecordItem和pks组装删除语句
     *
     * @param item
     * @param pks
     * @return
     */
    public static String getDelSql(OceanBaseRecordItem item, List<JdbcColumn> pks) {
        String sql = String.format("delete from %s.%s", item.getRecord().getSourceInfo().getDatabase(), item.getRecord().getSourceInfo().getTable());
        StringBuilder sb = new StringBuilder(sql);
        sb.append(WHERE);
        sb.append(pks.stream().map(pk -> pk.getName() + "=?").collect(Collectors.joining(AND)));
        return sb.toString();
    }

    /**
     * 根据OceanBaseRecordItem和pks组装更新语句
     *
     * @param item
     * @param pks
     * @return
     */
    public static String getUpdateSql(OceanBaseRecordItem item, List<JdbcColumn> pks) {

        String sql = String.format("update %s.%s", item.getRecord().getSourceInfo().getDatabase(), item.getRecord().getSourceInfo().getTable());
        StringBuilder sb = new StringBuilder(sql);
        List<String> columns = new ArrayList<>();
        for (Map.Entry<String, Object> entry : item.getRecord().getLogMessageFieldsAfter().entrySet()) {
            columns.add(entry.getKey() + "=?");
        }
        sb.append(" set ");
        sb.append(columns.stream().collect(Collectors.joining(",")));
        sb.append(WHERE);
        sb.append(pks.stream().map(pk -> pk.getName() + "=?").collect(Collectors.joining(AND)));
        return sb.toString();
    }

}
