package com.zzvcom.cdc.mysql.sink.batch.buff;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author yujikuan
 * @Classname BatchBuffer
 * @Description 批量buffer
 * @Date 2023/8/14 14:56
 */
public class BatchBuffer<T extends Serializable> implements java.io.Serializable {


    private static final long serialVersionUID = 8872757922232267391L;
    private final LinkedBlockingQueue<T> blockingQueue;

    public BatchBuffer(int size) {
        blockingQueue = new LinkedBlockingQueue<>(size);
    }

    /**
     * 堵塞获取
     * @return
     * @throws InterruptedException
     */
    public T get() throws InterruptedException {
        return blockingQueue.take();
    }

    /**
     * 堵塞放入
     * @param t
     * @throws InterruptedException
     */
    public void put(T t) throws InterruptedException {
        blockingQueue.put(t);
    }

    public int getSize() {
        return blockingQueue.size();
    }

    /**
     * 非堵塞争取多获取几个
     * @param n 获取的最大条数
     * @return
     */
    public List<T> get(int n) {
        List<T> result = new ArrayList<>(n);
        blockingQueue.drainTo(result, n);
        return result;
    }

}
