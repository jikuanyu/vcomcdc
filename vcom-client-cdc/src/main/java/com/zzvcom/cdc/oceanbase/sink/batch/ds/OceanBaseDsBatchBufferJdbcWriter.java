package com.zzvcom.cdc.oceanbase.sink.batch.ds;

import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariDataSource;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.oceanbase.sink.OceanBaseRecordItem;
import com.zzvcom.cdc.oceanbase.sink.batch.OceanBaseBatchJdbcWriter;
import com.zzvcom.cdc.util.conn.ds.JdbcDataSource;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * @author yujikuan
 * @Classname OceanBaseDsBatchBufferJdbcWriter
 * @Description 使用数据源，buf 生产和消费者写入
 * @Date 2023/8/22 19:34
 */
public class OceanBaseDsBatchBufferJdbcWriter implements OceanBaseBatchJdbcWriter {

    private static final Logger log = LoggerFactory.getLogger(OceanBaseDsBatchBufferJdbcWriter.class);

    private JdbcInfo jdbcInfo;
    private final ArrayList<OceanBaseRecordItem> batch;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient ThreadPoolExecutor threadPoolExecutor;
    private OceanbaseDsBatchBufferProcess oceanbaseDsBatchBufferProcess;

    public OceanBaseDsBatchBufferJdbcWriter(JdbcInfo jdbcInfo) {
        this.jdbcInfo = jdbcInfo;
        this.batch = new ArrayList<>();
    }

    /**
     * 初始化使用
     *
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        oceanbaseDsBatchBufferProcess = new OceanbaseDsBatchBufferProcess(jdbcInfo);
        this.scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("oceanbasedsbuffbatchJDBCWriter"));
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (OceanBaseDsBatchBufferJdbcWriter.this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (InterruptedException e) {
                                        log.warn("DsBatchBufferJdbcWriter InterruptedException");
                                        Thread.currentThread().interrupt();
                                    }

                                }
                            }
                        },
                        jdbcInfo.getBatchSinkOption().getBatchIntervalMs(),
                        jdbcInfo.getBatchSinkOption().getBatchIntervalMs(),
                        TimeUnit.MILLISECONDS);

        threadPoolExecutor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        threadPoolExecutor.execute(() -> oceanbaseDsBatchBufferProcess.startJob());
    }

    private void checkFlushException() {
        if (oceanbaseDsBatchBufferProcess.isFlushException()) {
            throw FatalException.of("ob dsBatchBufferProcess.isFlushException");
        }
    }

    private synchronized void flush() throws InterruptedException {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        oceanbaseDsBatchBufferProcess.addCopy(batch);
        batch.clear();
        batchCount = 0;
    }

    /**
     * 批量执行
     *
     * @param record
     */
    @Override
    public synchronized void addToBatch(OceanBaseRecordItem record) throws Exception {
        batch.add(record);
        batchCount++;
        if (batchCount >= jdbcInfo.getBatchSinkOption().getBatchSize()) {
            flush();
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
            oceanbaseDsBatchBufferProcess.close();
            if (threadPoolExecutor != null) {
                threadPoolExecutor.shutdownNow();
            }
        }
    }
}
