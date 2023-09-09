package com.zzvcom.cdc.mysql.sink.batch.buff;

import cn.hutool.core.collection.CollUtil;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.mysql.simple.TableItem;
import com.zzvcom.cdc.mysql.sink.batch.BatchTools;
import com.zzvcom.cdc.util.conn.JdbcConnectionProvider;
import com.zzvcom.cdc.util.conn.VcomJdbcConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yujikuan
 * @Classname BatchBufferWriter
 * @Description 从 buffer
 * @Date 2023/8/14 15:00
 */
public class BatchBufferProcess implements java.io.Serializable {

    private static final Logger log = LoggerFactory.getLogger(BatchBufferProcess.class);
    private static final long serialVersionUID = -8132898227533848735L;
    private final BatchBuffer<ArrayList<TableItem>> buffer;

    private final VcomJdbcConnectionProvider connectionProvider;
    private int maxTryNum;

    private transient volatile boolean closed = false;
    private transient volatile boolean isFlushException = false;

    public BatchBufferProcess(int size, VcomJdbcConnectionProvider connectionProvider, int maxTryNum) {
        buffer = new BatchBuffer<>(size);
        this.connectionProvider = connectionProvider;
        this.maxTryNum = maxTryNum;
    }


    /**
     * 生成者,添加
     *
     * @param items
     * @throws InterruptedException
     */
    public void addCopy(List<TableItem> items) throws InterruptedException {
        final ArrayList<TableItem> tableItems = CollUtil.newArrayList(items);
        buffer.put(tableItems);
    }

    /**
     * 消费者
     * 并线程调用。
     *
     * @see Thread#run()
     */
    public void startJob() {

        while (!isFlushException && !closed) {
            try {
                BatchTools batchTools = new BatchTools(buffer.get(), connectionProvider, maxTryNum);
                batchTools.flush();
            } catch (IOException ioException) {
                log.error("jdbc插入到目标数据库目标异常", ioException);
                isFlushException = true;
                throw FatalException.of("多次执行，依然无法处理，致命异常。");
            } catch (InterruptedException e) {
                log.error("被中断", e);
                Thread.currentThread().interrupt();
            }
        }
    }


    public JdbcConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    public void close() {
        if (!closed) {
            closed = true;
            log.info("调用了关闭，停止消费");
        }
    }

    public boolean isFlushException() {
        return isFlushException;
    }
}
