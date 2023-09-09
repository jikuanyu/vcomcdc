package com.zzvcom.cdc.cfg;

/**
 * @author yujikuan
 * @Classname BatchSinkOption
 * @Description 批量插入配置项
 * @Date 2023/8/8 20:20
 */
public class BatchSinkOption implements java.io.Serializable{
    private static final long serialVersionUID = 8620851958562459882L;

    /**
     * 1直接模式
     */
    public static final int  MODEL_DIRECT=1;
    /**
     * 2生产者消费者模式
     */
    public static final int  MODEL_BUFF=2;
    /**
     * 3生产或消费者模式和数据库连接池
     */
    public static final int  MODEL_BUFF_POOL=3;
    /**
     * 执行异常的最大重试次数
     */
    private  int maxTryNum;
    /**
     * 每批插入打最大大小
     */
    private int batchSize;

    /**
     * 批量定时的频率
     */
    private long batchIntervalMs;

     /**
     *  1、直接模式 2生产者消费者模式
      * dest.jdbc.batch.model=2
     */
    private int model;

    /**
     * 2生产消费者模式的时候,缓存的容量多少批
     */
    private int modelBatchBuffSize;


    public int getModel() {
        return model;
    }

    public void setModel(int model) {
        this.model = model;
    }

    public int getModelBatchBuffSize() {
        return modelBatchBuffSize;
    }

    public void setModelBatchBuffSize(int modelBatchBuffSize) {
        this.modelBatchBuffSize = modelBatchBuffSize;
    }

    public int getMaxTryNum() {
        return maxTryNum;
    }

    public void setMaxTryNum(int maxTryNum) {
        this.maxTryNum = maxTryNum;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public void setBatchIntervalMs(long batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
    }

    @Override
    public String toString() {
        return "BatchSinkOption{" +
                "maxTryNum=" + maxTryNum +
                ", batchSize=" + batchSize +
                ", batchIntervalMs=" + batchIntervalMs +
                ", model=" + model +
                ", modelBatchBuffSize=" + modelBatchBuffSize +
                '}';
    }
}
