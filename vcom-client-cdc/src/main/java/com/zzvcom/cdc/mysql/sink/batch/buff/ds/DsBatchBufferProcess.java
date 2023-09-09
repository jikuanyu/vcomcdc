package com.zzvcom.cdc.mysql.sink.batch.buff.ds;

import cn.hutool.core.collection.CollUtil;
import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariDataSource;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.mysql.simple.TableItem;
import com.zzvcom.cdc.mysql.sink.batch.BatchTools;
import com.zzvcom.cdc.mysql.sink.batch.buff.BatchBuffer;
import com.zzvcom.cdc.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author yujikuan
 * @Classname DsBatchBufferProcess
 * @Description 数据源的处理方式, 生产者消费模式。
 * @Date 2023/8/17 15:13
 */
public class DsBatchBufferProcess implements java.io.Serializable {

    private static final Logger log = LoggerFactory.getLogger(DsBatchBufferProcess.class);
    private static final long serialVersionUID = 1860651587679484813L;
    private final BatchBuffer<ArrayList<TableItem>> buffer;
    private final transient HikariDataSource dataSource;
    private JdbcInfo jdbcInfo;
    private final transient ThreadPoolExecutor threadPoolExecutor;
    private transient volatile boolean flushException;
    private transient volatile boolean closed = false;

    public DsBatchBufferProcess(HikariDataSource dataSource, JdbcInfo jdbcInfo) {
        this.dataSource = dataSource;
        this.jdbcInfo = jdbcInfo;
        buffer = new BatchBuffer<>(jdbcInfo.getBatchSinkOption().getModelBatchBuffSize());
        threadPoolExecutor = CommonUtil.newCacheThreadPool(4, 0L);
    }

    public void startJob() {

        while (!flushException && !closed) {
            try {
                //堵塞获取一个
                final ArrayList<TableItem> tableItems = buffer.get();
                //尝试获取更多
                List<ArrayList<TableItem>> batchList = buffer.get(jdbcInfo.getBatchSinkOption().getModelBatchBuffSize());
                batchList.add(tableItems);
                log.warn("ds jdbcInfo={}", jdbcInfo);
                flush(batchList);
            } catch (InterruptedException e) {
                log.error("startJob 被中断", e);
                flushException = true;
                Thread.currentThread().interrupt();
            }
        }
    }

    private void flush(List<ArrayList<TableItem>> batchList) {

        for (int i = 0; i <= jdbcInfo.getBatchSinkOption().getMaxTryNum(); i++) {
            try {
                jobDetail(batchList);
                batchList.clear();
                break;
            } catch (FatalException | ExecutionException fe) {
                if (i >= jdbcInfo.getBatchSinkOption().getMaxTryNum()) {
                    flushException = true;
                    log.error("ds 尝试了{}次，无法进行了。", i);
                    throw FatalException.of("已经尝试最大次数依然失败ds");
                }
                log.error("ds FatalException 处理的异常， 尝试次数={}", i, fe);
                try {
                    Thread.sleep(500L * i);
                } catch (InterruptedException ex) {
                    log.error("ds start Job被中断,sleep,重试 次 " + i, ex);
                    flushException = true;
                    Thread.currentThread().interrupt();
                }
            } catch (InterruptedException e) {
                log.error("ds start Job被中断", e);
                flushException = true;
                Thread.currentThread().interrupt();
            }
        }

    }

    private void jobDetail(List<ArrayList<TableItem>> batchList) throws InterruptedException, ExecutionException, FatalException {
        //没有冲突批量执行
        if (isBatchProcessTableItem(batchList)) {
            parallelProcess(batchList);
        } else {
            for (List<TableItem> baths : batchList) {
                //按照顺序执行
                try (Connection connection = dataSource.getConnection()) {
                    BatchTools.attemptFlush(connection, baths);
                } catch (SQLException sqlException) {
                    log.error(" Serial jobDetail ds sqlException", sqlException);
                    throw FatalException.of("Serial,存在处理错误的异常");
                }
            }
        }
    }

    private void parallelProcess(List<ArrayList<TableItem>> batchList) throws InterruptedException, ExecutionException {
        //进行并行处理,如果不重复，可以并行执行
        List<CompletableFuture<Boolean>> futureList = new ArrayList<>(batchList.size());
        for (List<TableItem> baths : batchList) {
            final CompletableFuture<Boolean> booleanCompletableFuture = CompletableFuture.supplyAsync(() -> {
                try (Connection connection = dataSource.getConnection()) {
                    BatchTools.attemptFlush(connection, baths);
                } catch (SQLException sqlException) {
                    log.error("parallel jobDetail ds sqlException", sqlException);
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            }, threadPoolExecutor);
            futureList.add(booleanCompletableFuture);
        }
        log.warn("futureList size={}", futureList.size());
        final CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]));
        try {
            voidCompletableFuture.get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.error("parallel异常，需要人工处理TimeoutException", e);
            throw FatalException.of("parallel异常，需要人工处理TimeoutException");
        }
        log.warn("end futureList size={}", futureList.size());
        for (CompletableFuture<Boolean> booleanCompletableFuture : futureList) {
            if (Boolean.FALSE.equals(booleanCompletableFuture.get())) {
                throw FatalException.of("parallel异常,存在处理错误的异常");
            }
        }
    }


    public void addCopy(List<TableItem> items) throws InterruptedException {
        final ArrayList<TableItem> tableItems = CollUtil.newArrayList(items);
        buffer.put(tableItems);
    }

    public void close() {
        if (!closed) {
            closed = true;
            if (buffer.getSize() > 0) {
                log.warn("关闭的时候依旧还有数据没有处理完，dsbatchBufferProccess.getBuffer()={}", buffer);
            }
            threadPoolExecutor.shutdown();
        }
    }

    public boolean isFlushException() {
        return flushException;
    }

    private boolean isBatchProcessTableItem(List<ArrayList<TableItem>> batchList) {
        Set<TableItem> allSet = new HashSet<>();
        int listSize = 0;
        for (List<TableItem> tableItems : batchList) {
            allSet.addAll(tableItems);
            listSize += tableItems.size();
        }
        return listSize == allSet.size();
    }
}
