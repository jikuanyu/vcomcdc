package com.zzvcom.cdc.ex;

/**
 * @author yujikuan
 * @Classname ConfigureException
 * @Description 配置异常
 * @Date 2023/8/18 9:47
 */
public class ConfigureException extends Exception {

    private static final long serialVersionUID = 970064736617363316L;

    public ConfigureException() {
        super();
    }

    public ConfigureException(String msg) {
        super(msg);
    }

    public static ConfigureException of(String msg) {
        return new ConfigureException(msg);
    }

}
