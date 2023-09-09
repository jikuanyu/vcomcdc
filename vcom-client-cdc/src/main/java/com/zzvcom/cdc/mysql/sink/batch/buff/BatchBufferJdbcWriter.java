package com.zzvcom.cdc.mysql.sink.batch.buff;

import com.zzvcom.cdc.cfg.BatchSinkOption;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.mysql.simple.TableItem;
import com.zzvcom.cdc.mysql.sink.batch.BatchJdbcWriter;
import com.zzvcom.cdc.util.conn.VcomJdbcConnectionProvider;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author yujikuan
 * @Classname MySqlBatchWriter
 * @Description 生成者消费者模式插入数据库 (一个消费者迭代)
 * @Date 2023/8/8 15:30
 */
public class BatchBufferJdbcWriter implements BatchJdbcWriter {


    private static final Logger log = LoggerFactory.getLogger(BatchBufferJdbcWriter.class);
    private static final long serialVersionUID = -8839277299341611872L;
    private final List<TableItem> batch;

    private transient int batchCount = 0;
    private BatchSinkOption batchSinkOption;

    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;

    private BatchBufferProcess batchBufferProcess;
    private transient ThreadPoolExecutor threadPoolExecutor;

    public BatchBufferJdbcWriter(VcomJdbcConnectionProvider connectionProvider) {
        this.batch = new ArrayList<>();
        //批量写入配置
        batchSinkOption = connectionProvider.getJdbcInfo().getBatchSinkOption();
        batchBufferProcess = new BatchBufferProcess(batchSinkOption.getModelBatchBuffSize(), connectionProvider, batchSinkOption.getMaxTryNum());
    }

    @Override
    public void open() throws Exception {
        batchBufferProcess.getConnectionProvider().getOrEstablishConnection();
        this.scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("buffbatchJDBCWriter"));
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (BatchBufferJdbcWriter.this) {
                                if (!closed) {

                                    try {
                                        flush();
                                    } catch (InterruptedException e) {
                                        log.warn("BatchBufferJdbcWriter InterruptedException");
                                        Thread.currentThread().interrupt();
                                    }

                                }
                            }
                        },
                        batchSinkOption.getBatchIntervalMs(),
                        batchSinkOption.getBatchIntervalMs(),
                        TimeUnit.MILLISECONDS);

        threadPoolExecutor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        threadPoolExecutor.execute(() ->
                batchBufferProcess.startJob()
        );
    }

    /**
     * 添加
     *
     * @param record
     * @throws IOException
     */
    @Override
    public synchronized void addToBatch(TableItem record) throws Exception {
        batch.add(record);
        batchCount++;
        if (batchCount >= batchSinkOption.getBatchSize()) {
            flush();
        }
    }


    private synchronized void flush() throws InterruptedException {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        batchBufferProcess.addCopy(batch);
        batch.clear();
        batchCount = 0;
    }


    private void checkFlushException() {
        if (batchBufferProcess.isFlushException()) {
            throw FatalException.of("Buffer保存异常");
        }
    }

    /**
     * 关闭连接,释放相关资源
     */
    @Override
    public synchronized void close() throws InterruptedException {
        if (!closed) {
            closed = true;
            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }
            if (batchCount > 0) {
                flush();
            }
            batchBufferProcess.close();
            if (threadPoolExecutor != null) {
                threadPoolExecutor.shutdownNow();
            }
        }
        //关闭其他
        batchBufferProcess.getConnectionProvider().closeConnection();
        checkFlushException();
    }
}
