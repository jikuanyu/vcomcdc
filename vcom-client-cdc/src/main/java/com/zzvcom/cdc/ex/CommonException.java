package com.zzvcom.cdc.ex;

/**
 * @author yujikuan
 * @Classname CommonException
 * @Description 一般异常
 * @Date 2023/8/18 9:36
 */
public class CommonException extends Exception {
    private static final long serialVersionUID = -5482411511103562002L;

    public CommonException() {
        super();
    }

    public CommonException(String msg) {
        super(msg);
    }

    public static CommonException of(String msg) {
        return new CommonException(msg);
    }
}
