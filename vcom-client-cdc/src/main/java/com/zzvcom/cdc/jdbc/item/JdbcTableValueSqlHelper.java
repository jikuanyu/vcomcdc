package com.zzvcom.cdc.jdbc.item;


import cn.hutool.core.collection.CollUtil;
import com.zzvcom.cdc.ex.FatalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yujikuan
 * @Classname JdbcTableValueSqlHelper
 * @Description 根据JdbcTableValue生成sql
 * @Date 2023/8/23 15:16
 */
public class JdbcTableValueSqlHelper {

    private static final Logger log = LoggerFactory.getLogger(JdbcTableValueSqlHelper.class);

    private JdbcTableValueSqlHelper() {
    }

    /**
     * insert sql
     *
     * @param tableItem
     * @return
     */
    public static String getInsertSql(JdbcTableValue tableItem) {
        String sql = "insert into " + tableItem.getDb() + "." + tableItem.getTableName();
        String nameSql = tableItem.getAfterColumns().stream().map(JdbcColumnValue::getName).collect(Collectors.joining(","));
        String endSql = tableItem.getAfterColumns().stream().map(item -> "?").collect(Collectors.joining(","));
        return sql + " (" + nameSql + ")" + "values(" + endSql + ")";
    }

    /**
     * 快照方式生成sql
     *
     * @param value
     * @return
     */
    public static String getReplaceInsertSqlByRead(JdbcTableValue value) {
        final String insertSql = getInsertSql(value);
        final String middleSql = " on duplicate key update ";
        String endSql = value.getAfterColumns().stream().map(item -> item.getName() + "=values(" + item.getName() + ")").collect(Collectors.joining(","));
        return insertSql + middleSql + endSql;
    }

    public static String getBatchDeleteSql(List<JdbcTableValue> list) {
        JdbcTableValue tableItem = list.get(0);
        String sql = "delete from " + tableItem.getDb() + "." + tableItem.getTableName() + " where ";
        if (tableItem.getPks().size() > 1) {
            throw FatalException.of("批量删除不支持，多个主键的情况");
        }
        if (tableItem.getPks() == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(sql);
        final JdbcColumnValue pkColumn = tableItem.getPks().get(0);
        sb.append(pkColumn.getName());
        sb.append(" in (");
        if (pkColumn.getValue() instanceof String) {
            sb.append(list.stream().map(a -> "'" + a.getPks().get(0).getValue() + "'").collect(Collectors.joining(",")));
        } else {
            sb.append(list.stream().map(a -> a.getPks().get(0).getValue().toString()).collect(Collectors.joining(",")));
        }
        sb.append(")");
        return sb.toString();
    }

    public static String getDeleteSql(JdbcTableValue jdbcTableValue) {
        if (CollUtil.isEmpty(jdbcTableValue.getPks())) {
            log.warn("不存在主键，无法执行删除操作。jdbcTableValue={}", jdbcTableValue);
            return null;
        }
        String sql = String.format("delete from %s.%s", jdbcTableValue.getDb(), jdbcTableValue.getTableName());
        StringBuilder sb = new StringBuilder(sql);
        sb.append(" where ");
        sb.append(jdbcTableValue.getPks().stream().map(pk -> pk.getName() + "=?").collect(Collectors.joining(" and ")));
        return sb.toString();
    }
}
