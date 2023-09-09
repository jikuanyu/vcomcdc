package com.zzvcom.cdc.mysql.simple;

import io.debezium.data.Envelope;

import java.util.List;
import java.util.Objects;

/**
 * 表对象
 * @author Administrator
 */
public class TableItem implements java.io.Serializable{


    private static final long serialVersionUID = 4727667312788863963L;
    /**
     * 数据库名字
     */
    private String db;

    /**
     * 表的名称
     */
    private String table;

    /**
     * 操作前
     */
    private List<ColumnItem> before;
    /**
     * 操作后
     */
    private List<ColumnItem> after;
    /**
     * 主键
     */
    private List<ColumnItem> pks;
    /**
     * 是否数据库中已经存在
     */
    private boolean exist;

    /**
     * 操作类型
     */
    private    Envelope.Operation op;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<ColumnItem> getBefore() {
        return before;
    }

    public void setBefore(List<ColumnItem> before) {
        this.before = before;
    }

    public List<ColumnItem> getAfter() {
        return after;
    }

    public void setAfter(List<ColumnItem> after) {
        this.after = after;
    }

    public    Envelope.Operation getOp() {
        return op;
    }

    public void setOp(Envelope.Operation op) {
        this.op = op;
    }

    public List<ColumnItem> getPks() {
        return pks;
    }

    public void setPks(List<ColumnItem> pks) {
        this.pks = pks;
    }

    public boolean isExist() {
        return exist;
    }

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    @Override
    public String toString() {
        return "TableItem{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", pks=" + pks +
                ", exist=" + exist +
                ", op=" + op +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableItem)) return false;
        TableItem tableItem = (TableItem) o;
        return getDb().equals(tableItem.getDb()) && getTable().equals(tableItem.getTable()) && getPks().equals(tableItem.getPks());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDb(), getTable(), getPks());
    }
}
