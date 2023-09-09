package com.zzvcom.cdc.mysql.sink.batch.direct;

import com.zzvcom.cdc.cfg.BatchSinkOption;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.mysql.simple.TableItem;
import com.zzvcom.cdc.mysql.sink.batch.BatchJdbcWriter;
import com.zzvcom.cdc.mysql.sink.batch.BatchTools;
import com.zzvcom.cdc.util.conn.VcomJdbcConnectionProvider;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yujikuan
 * @Classname MySqlBatchWriter
 * @Description 直接模式的，收集多个进行插入或则动时间进行插入。
 * @Date 2023/8/8 15:30
 * 参见org.apache.flink.connector.jdbc.internal.JdbcOutputFormat 自己自定义实现
 */
public class SimpleBatchJdbcWriter implements BatchJdbcWriter {

    private static final Logger log = LoggerFactory.getLogger(SimpleBatchJdbcWriter.class);
    private static final long serialVersionUID = 9126900979792478940L;
    private final List<TableItem> batch;
    private final VcomJdbcConnectionProvider connectionProvider;
    private transient int batchCount = 0;
    private BatchSinkOption batchSinkOption;

    private transient volatile boolean closed = false;
    private transient volatile boolean isFlushException = false;


    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;


    public SimpleBatchJdbcWriter(final VcomJdbcConnectionProvider connectionProvider) {
        this.batch = new ArrayList<>();
        this.connectionProvider = connectionProvider;
        //批量写入配置
        batchSinkOption = connectionProvider.getJdbcInfo().getBatchSinkOption();
    }

    @Override
    public void open() throws Exception {
        this.connectionProvider.getOrEstablishConnection();
        this.scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("SimpleBatchJdbcWriter"));
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (SimpleBatchJdbcWriter.this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        isFlushException = true;
                                        log.error("simple定时刷新异常", e);
                                    }
                                }
                            }
                        },
                        batchSinkOption.getBatchIntervalMs(),
                        batchSinkOption.getBatchIntervalMs(),
                        TimeUnit.MILLISECONDS);
    }

    /**
     * 添加
     *
     * @param record
     * @throws IOException
     */
    @Override
    public synchronized void addToBatch(TableItem record) throws IOException {
        batch.add(record);
        batchCount++;
        if (batchCount >= batchSinkOption.getBatchSize()) {
            flush();
        }
    }

    private synchronized void flush() throws IOException {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        BatchTools batchTools = new BatchTools(batch, connectionProvider, batchSinkOption.getMaxTryNum());
        batchTools.flush();
        batch.clear();
        batchCount = 0;
    }


    private void checkFlushException() {
        if (isFlushException) {
            throw FatalException.of("存在刷新异常");
        }
    }

    /**
     * 关闭连接,释放相关资源
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    throw FatalException.of("Writing records to JDBC failed.", e);
                }
            }
        }
        //关闭其他
        connectionProvider.closeConnection();
        checkFlushException();
    }
}
