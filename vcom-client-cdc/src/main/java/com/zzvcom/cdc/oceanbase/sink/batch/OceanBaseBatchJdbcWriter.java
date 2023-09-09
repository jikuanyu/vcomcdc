package com.zzvcom.cdc.oceanbase.sink.batch;


import com.zzvcom.cdc.oceanbase.sink.OceanBaseRecordItem;

import java.io.Serializable;

/**
 * @author yujikuan
 * @Classname DsOceanBaseJdbcWriter
 * @Description oceanbase to jdbc ds
 * @Date 2023/8/22 19:23
 */
public interface OceanBaseBatchJdbcWriter extends Serializable {
    /**
     * 初始化使用
     *
     * @throws Exception
     */

    public void open() throws Exception;

    /**
     * 批量执行
     *
     * @param record
     */
    public void addToBatch(OceanBaseRecordItem record)throws  Exception;

    /**
     * 关闭连接,释放相关资源
     */

    public void close() throws InterruptedException;
}