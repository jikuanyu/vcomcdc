package com.zzvcom.cdc.jdbc.item;

import com.zzvcom.cdc.jdbc.JdbcColumn;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author yujikuan
 * @Classname JdbcTableValue
 * @Description 特定一个表的行，目前oceanbase使用。
 * @Date 2023/8/23 10:44
 */
public class JdbcTableValue implements java.io.Serializable {
    private static final long serialVersionUID = 6544063703592826922L;

    public static final int OP_INSERT = 1;
    public static final int OP_UPDATE = 2;
    public static final int OP_DELETE = 3;
    public static final int OP_READ = 4;
    /**
     * 数据库名字
     */
    private String db;
    /**
     * 表名称
     */
    private String tableName;
    /**
     * 变化前
     */
    private List<JdbcColumnValue> beforeColumns;

    /**
     * 变化后
     */
    private List<JdbcColumnValue> afterColumns;

    /**
     * 主键
     */
    private List<JdbcColumnValue> pks;

    /**
     * 操作类型
     */
    private int op;

    public int getOp() {
        return op;
    }

    public void setOp(int op) {
        this.op = op;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<JdbcColumnValue> getBeforeColumns() {
        return beforeColumns;
    }

    public void setBeforeColumns(List<JdbcColumnValue> beforeColumns) {
        this.beforeColumns = beforeColumns;
    }

    public List<JdbcColumnValue> getAfterColumns() {
        return afterColumns;
    }

    public void setAfterColumns(List<JdbcColumnValue> afterColumns) {
        this.afterColumns = afterColumns;
    }

    public List<JdbcColumnValue> getPks() {
        return pks;
    }

    public void setPks(List<JdbcColumnValue> pks) {
        this.pks = pks;
    }

    public void makePksByAfter() {
        if (this.afterColumns == null) {
            return;
        }
        this.pks = this.afterColumns.stream().filter(JdbcColumn::isPK).collect(Collectors.toList());
    }

    public void makePksByBefore() {
        if (this.beforeColumns == null) {
            return;
        }
        this.pks = this.beforeColumns.stream().filter(JdbcColumn::isPK).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "JdbcTableValue{" +
                "db='" + db + '\'' +
                ", tableName='" + tableName + '\'' +
                ", beforeColumns=" + beforeColumns +
                ", afterColumns=" + afterColumns +
                ", pks=" + pks +
                ", op=" + op +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JdbcTableValue)) return false;
        JdbcTableValue that = (JdbcTableValue) o;
        return getDb().equals(that.getDb()) && getTableName().equals(that.getTableName()) && getPks().equals(that.getPks());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDb(), getTableName(), getPks());
    }


}
