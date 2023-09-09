package com.zzvcom.cdc.ex;

/**
 * @author yujikuan
 * @Classname HandleException
 * @Description 处理异常，暂停的异常。
 * @Date 2023/8/18 9:40
 */
public class HandleException extends Exception {

    public HandleException() {
        super();
    }

    public HandleException(String msg) {
        super(msg);
    }

    public HandleException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public static HandleException of(String msg) {
        return new HandleException(msg);
    }



}
