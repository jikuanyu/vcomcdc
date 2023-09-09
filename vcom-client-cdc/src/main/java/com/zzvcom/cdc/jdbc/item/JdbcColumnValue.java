package com.zzvcom.cdc.jdbc.item;

import com.zzvcom.cdc.jdbc.JdbcColumn;

import java.util.Objects;

/**
 * @author yujikuan
 * @Classname JdbcColumnValue
 * @Description 转成带有值的表烂
 * @Date 2023/8/23 10:39
 */
public class JdbcColumnValue extends JdbcColumn {
    private static final long serialVersionUID = 8712827449586245001L;



    public static JdbcColumnValue getJdbcColumnValue(JdbcColumn column) {
        JdbcColumnValue value = new JdbcColumnValue();
        value.setName(column.getName());
        value.setDataType(column.getDataType());
        value.setPK(column.isPK());
        value.setTypeName(column.getTypeName());
        return value;
    }

    /**
     * 栏的值
     */
    private Object value;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JdbcColumnValue)) return false;
        if (!super.equals(o)) return false;
        JdbcColumnValue that = (JdbcColumnValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public String toString() {
        return "JdbcColumnValue{" +
                "value=" + value +
                "} " + super.toString();
    }
}
