package com.zzvcom.cdc.jdbc;

import java.util.Objects;

/**
 * @author yujikuan
 */
public class JdbcColumn implements java.io.Serializable {
    private static final long serialVersionUID = 3298656925924318942L;
    /**
     * 栏目的名称
     */
    private String name;
    /**
     * 数据类型
     */
    private int dataType;
    /**
     * 是否是主键
     */
    private boolean isPK;
    /**
     * typeName
     */
    private String typeName;

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getDataType() {
        return dataType;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }


    public boolean isPK() {
        return isPK;
    }

    @Override
    public String toString() {
        return "JdbcColumn{" +
                "name='" + name + '\'' +
                ", dataType=" + dataType +
                ", isPK=" + isPK +
                ", typeName='" + typeName + '\'' +
                '}';
    }

    public void setPK(boolean pk) {
        isPK = pk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcColumn that = (JdbcColumn) o;
        return dataType == that.dataType && isPK == that.isPK && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, isPK);
    }
}
