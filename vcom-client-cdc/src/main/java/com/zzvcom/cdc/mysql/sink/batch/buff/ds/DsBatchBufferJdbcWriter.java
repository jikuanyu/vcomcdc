package com.zzvcom.cdc.mysql.sink.batch.buff.ds;

import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariDataSource;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.mysql.simple.TableItem;
import com.zzvcom.cdc.mysql.sink.batch.BatchJdbcWriter;
import com.zzvcom.cdc.util.conn.ds.JdbcDataSource;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * @author yujikuan
 * @Classname DsBatchBufferJdbcWriter
 * @Description 使用数据库连接、生产消费模式实现
 * @Date 2023/8/17 14:31
 */
public class DsBatchBufferJdbcWriter implements BatchJdbcWriter {
    private static final Logger log = LoggerFactory.getLogger(DsBatchBufferJdbcWriter.class);

    private JdbcInfo jdbcInfo;

    private final ArrayList<TableItem> batch;

    private transient int batchCount = 0;


    private transient volatile boolean closed = false;


    private transient HikariDataSource dataSource;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient ThreadPoolExecutor threadPoolExecutor;
    private DsBatchBufferProcess dsBatchBufferProcess;


    public DsBatchBufferJdbcWriter(JdbcInfo jdbcInfo) {
        this.jdbcInfo = jdbcInfo;
        this.batch = new ArrayList<>();
    }

    @Override
    public void open() {
        dataSource = JdbcDataSource.getDataSource(jdbcInfo);
        dsBatchBufferProcess = new DsBatchBufferProcess(dataSource, jdbcInfo);
        this.scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("buffbatchJDBCWriter"));
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (DsBatchBufferJdbcWriter.this) {
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
        threadPoolExecutor.execute(() -> dsBatchBufferProcess.startJob());

    }

    private void checkFlushException() {
        if (dsBatchBufferProcess.isFlushException()) {
            throw FatalException.of("dsBatchBufferProcess.isFlushException");
        }
    }

    @Override
    public synchronized void addToBatch(TableItem record) throws Exception {
        batch.add(record);
        batchCount++;
        if (batchCount >= jdbcInfo.getBatchSinkOption().getBatchSize()) {
            flush();
        }
    }

    private synchronized void flush() throws InterruptedException {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        dsBatchBufferProcess.addCopy(batch);
        batch.clear();
        batchCount = 0;
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
            dsBatchBufferProcess.close();
            if (threadPoolExecutor != null) {
                threadPoolExecutor.shutdownNow();
            }
        }
        dataSource.close();
    }
}
