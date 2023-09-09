package com.zzvcom.cdc.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author yujikuan
 * 源库中表的定义
 */
public class JdbcTable implements java.io.Serializable {

    private static final long serialVersionUID = -974334313006091026L;
    /**
     * 数据库名字
     */
    private String db;
    /**
     * 表名称
     */
    private String tableName;

    /**
     * 字段名称
     */
    private List<JdbcColumn> columnList;

    /**
     * 字段名称Map
     */
    private Map<String, JdbcColumn> columnMap;


    /**
     * 添加column
     *
     * @param column
     */
    public void addColumn(JdbcColumn column) {
        if (columnList == null) {
            columnList = new ArrayList<>();
        }
        columnList.add(column);
    }

    /**
     * 填充columnMap
     */
    public void fixColumnMap() {
        if (columnList == null) {
            return;
        }
        columnMap = columnList.stream().collect(Collectors.toMap(JdbcColumn::getName, Function.identity()));
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

    public List<JdbcColumn> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<JdbcColumn> columnList) {
        this.columnList = columnList;
    }

    public Map<String, JdbcColumn> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, JdbcColumn> columnMap) {
        this.columnMap = columnMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcTable table = (JdbcTable) o;
        return Objects.equals(db, table.db) && Objects.equals(tableName, table.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, tableName);
    }

    @Override
    public String toString() {
        return "JdbcTable{" +
                "db='" + db + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columnList=" + columnList +
                ", columnMap=" + columnMap +
                '}';
    }
}
