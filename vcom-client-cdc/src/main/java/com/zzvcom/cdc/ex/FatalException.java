package com.zzvcom.cdc.ex;

/**
 * @author yujikuan
 * @Classname FatalException
 * @Description 无法恢复的异常
 * @Date 2023/8/18 9:59
 */
public class FatalException extends RuntimeException {
    public FatalException() {
        super();
    }

    public FatalException(String msg) {
        super(msg);
    }

    public FatalException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public static FatalException of(String msg) {
        return new FatalException(msg);
    }

    public static FatalException of(String msg, Throwable cause) {
        return new FatalException(msg, cause);
    }
}
