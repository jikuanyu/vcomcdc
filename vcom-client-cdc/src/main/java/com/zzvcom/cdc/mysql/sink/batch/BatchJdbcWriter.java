package com.zzvcom.cdc.mysql.sink.batch;

import com.zzvcom.cdc.mysql.simple.TableItem;

import java.io.Serializable;

/**
 * @author yujikuan
 * @Classname BatchJdbcWriter
 * @Description 批量插入框架
 * @Date 2023/8/14 16:58
 */
public interface BatchJdbcWriter extends Serializable {
    /**
     * 初始化使用
     *
     * @throws Exception
     */
    public void open() throws Exception;

    public void addToBatch(TableItem record) throws Exception;

    /**
     * 关闭连接,释放相关资源
     */
    public void close() throws InterruptedException;

}
