package com.zzvcom.cdc.mysql.simple;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;

import java.util.Objects;

/**
 * @author yujikuan
 */
public class ColumnItem implements java.io.Serializable {
    private static final long serialVersionUID = -2513369748509062224L;
    /**
     * 列的名称
     */
    private String name;
    /**
     * 列的类型
     */
    private Schema.Type type;
    /**
     * 列的类型 name of this schema
     */
    private String schemaName;
    /**
     * 列的值
     */
    private Object value;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Schema.Type getType() {
        return type;
    }

    public void setType(Schema.Type type) {
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public String toString() {
        return "ColumnItem{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", schemaName='" + schemaName + '\'' +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnItem)) return false;
        ColumnItem that = (ColumnItem) o;
        return getName().equals(that.getName()) && Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getValue());
    }
}
