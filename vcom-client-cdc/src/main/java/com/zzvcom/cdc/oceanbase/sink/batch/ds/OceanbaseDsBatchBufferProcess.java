package com.zzvcom.cdc.oceanbase.sink.batch.ds;

import cn.hutool.core.collection.CollUtil;
import com.oceanbase.oms.logmessage.DataMessage;
import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariDataSource;
import com.zzvcom.cdc.cfg.JdbcInfo;
import com.zzvcom.cdc.ex.FatalException;
import com.zzvcom.cdc.jdbc.JdbcColumn;
import com.zzvcom.cdc.jdbc.JdbcTable;
import com.zzvcom.cdc.jdbc.item.JdbcColumnValue;
import com.zzvcom.cdc.jdbc.item.JdbcTableValue;
import com.zzvcom.cdc.jdbc.item.JdbcTableValueSqlHelper;
import com.zzvcom.cdc.mysql.sink.batch.buff.BatchBuffer;
import com.zzvcom.cdc.oceanbase.sink.OceanBaseRecordItem;
import com.zzvcom.cdc.oceanbase.util.ObLogConvert;
import com.zzvcom.cdc.util.CommonUtil;
import com.zzvcom.cdc.util.JdbcConnnUtil;
import com.zzvcom.cdc.util.conn.ds.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author yujikuan
 * @Classname OceanbaseDsBatchBufferProcess
 * @Description oceanbase批量写入处理方式。
 * @Date 2023/8/22 19:50
 */
public class OceanbaseDsBatchBufferProcess implements java.io.Serializable {
    private static final Logger log = LoggerFactory.getLogger(OceanbaseDsBatchBufferProcess.class);
    private static final long serialVersionUID = 4643887348800674426L;
    private final BatchBuffer<ArrayList<OceanBaseRecordItem>> buffer;
    private final transient HikariDataSource dataSource;
    private JdbcInfo jdbcInfo;
    private final transient ThreadPoolExecutor threadPoolExecutor;
    private transient volatile boolean flushException;
    private transient volatile boolean closed = false;

    public OceanbaseDsBatchBufferProcess(JdbcInfo jdbcInfo) {
        this.dataSource = JdbcDataSource.getDataSource(jdbcInfo);
        this.jdbcInfo = jdbcInfo;
        buffer = new BatchBuffer<>(jdbcInfo.getBatchSinkOption().getModelBatchBuffSize());
        threadPoolExecutor = CommonUtil.newCacheThreadPool(4, 0L);
    }

    /**
     * 异步执行
     */
    public void startJob() {
        while (!flushException && !closed) {
            try {
                //先堵塞获取
                final ArrayList<OceanBaseRecordItem> recordItems = buffer.get();
                //再次尽可能获取更多
                List<ArrayList<OceanBaseRecordItem>> batchList = buffer.get(jdbcInfo.getBatchSinkOption().getModelBatchBuffSize());
                batchList.add(recordItems);
                startJobDetail(batchList);

            } catch (InterruptedException e) {
                log.error("ob start Job buffer.get() 被中断", e);
                flushException = true;
                Thread.currentThread().interrupt();
            }

        }
    }

    private void startJobDetail(List<ArrayList<OceanBaseRecordItem>> batchList) {
        for (int i = 0; i <= jdbcInfo.getBatchSinkOption().getMaxTryNum(); i++) {
            try {
                log.warn("ds oceanbase jdbcInfo={}", jdbcInfo);
                flush(batchList);
                batchList.clear();
                break;
            } catch (FatalException fe) {
                if (i >= jdbcInfo.getBatchSinkOption().getMaxTryNum()) {
                    flushException = true;
                    log.warn("尝试了{}次，无法进行了。", i);
                }
                log.error("FatalException 处理的异常， 尝试次数={}", i, fe);
                try {
                    Thread.sleep(500L * i);
                } catch (InterruptedException ex) {
                    log.error("ob start Job被中断,sleep,重试 次 " + i, ex);
                    flushException = true;
                    Thread.currentThread().interrupt();
                }

            } catch (ExecutionException e) {
                log.error("ob startJob ExecutionException", e);
                flushException = true;
            } catch (InterruptedException e) {
                log.error("ob start Job被中断", e);
                flushException = true;
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isFlushException() {
        return flushException;
    }

    private void flush(List<ArrayList<OceanBaseRecordItem>> batchList) throws ExecutionException, InterruptedException {
        //所有的都是快照获取
        if (isAllSnapShot(batchList)) {
            //处理快照细节
            detailSnapShot(batchList);
        } else {
            log.info("----------------存在日志获取的方式-----------");
            //如果即有快照和日志，有出现的概率。
            List<ArrayList<JdbcTableValue>> convertObList = convertCommon(batchList);
            detailObCommons(convertObList);
        }
    }

    private List<ArrayList<JdbcTableValue>> convertCommon(List<ArrayList<OceanBaseRecordItem>> batchList) {
        List<ArrayList<JdbcTableValue>> convertObList = new ArrayList<>(batchList.size());
        //都存在逐个执行
        for (ArrayList<OceanBaseRecordItem> oceanBaseRecordItems : batchList) {
            ArrayList<JdbcTableValue> subList = new ArrayList<>(oceanBaseRecordItems.size());
            for (OceanBaseRecordItem item : oceanBaseRecordItems) {
                if (item.getRecord().isSnapshotRecord()) {
                    subList.add(convertSnapShotItem(item));
                } else {
                    subList.add(convertObLogItem(item));
                }
            }
            convertObList.add(subList);
        }
        return convertObList;
    }

    /**
     * 一批次的处理
     *
     * @param convertObList 待出处理的批次
     */
    private void detailObCommons(List<ArrayList<JdbcTableValue>> convertObList) {
        //暂时顺序执行。
        for (ArrayList<JdbcTableValue> jdbcTableValues : convertObList) {
            final Map<String, List<JdbcTableValue>> collect = jdbcTableValues.stream().collect(Collectors.groupingBy(item -> (item.getDb() + "." + item.getTableName()), Collectors.toList()));
            //每张表逐个处理
            for (Map.Entry<String, List<JdbcTableValue>> listEntry : collect.entrySet()) {
                final List<JdbcTableValue> tmpValues = listEntry.getValue();
                final Map<Integer, List<JdbcTableValue>> opGroup = tmpValues.stream().collect(Collectors.groupingBy(JdbcTableValue::getOp));
                try {
                    batchReplaceInsert(opGroup.get(JdbcTableValue.OP_READ));
                    batchReplaceInsert(opGroup.get(JdbcTableValue.OP_INSERT));
                    batchReplaceInsert(opGroup.get(JdbcTableValue.OP_UPDATE));
                } catch (SQLException sqlException) {
                    log.error("ob log  insert or update 需要顺序处理 sqlException ", sqlException);
                    throw FatalException.of("ob log  insert or update 需要顺序处理 sqlException");
                }
                delete(opGroup.get(JdbcTableValue.OP_DELETE));
            }
        }
    }

    private void delete(List<JdbcTableValue> deleteList) {
        if (CollUtil.isEmpty(deleteList)) {
            return;
        }
        //每天处理
        final JdbcTableValue jdbcTableValue = deleteList.get(0);
        if (jdbcTableValue.getPks().size() == 1) {
            try {
                try (Connection conn = dataSource.getConnection()) {
                    try (final Statement st = conn.createStatement()) {
                        final String batchDeleteSql = JdbcTableValueSqlHelper.getBatchDeleteSql(deleteList);
                        log.debug("batchDeleteSql={}", batchDeleteSql);
                        st.addBatch(batchDeleteSql);
                        st.executeBatch();
                    }
                }
            } catch (SQLException sqlException) {
                log.error("ob log del 需要顺序处理 sqlException ", sqlException);
                throw FatalException.of("ob log  del 需要顺序处理 sqlException");
            }
        } else {
            ///逐个删除
            String deleteSql = JdbcTableValueSqlHelper.getDeleteSql(jdbcTableValue);
            log.debug("deleteSql={}", deleteSql);
            if (deleteSql == null) {
                return;
            }
            try {
                try (Connection conn = dataSource.getConnection()) {
                    try (PreparedStatement pst = conn.prepareStatement(deleteSql)) {
                        for (JdbcTableValue tableValue : deleteList) {
                            int i = 0;
                            for (JdbcColumnValue pk : tableValue.getPks()) {
                                JdbcConnnUtil.setField(pst, pk, pk.getValue(), i++);
                            }
                            pst.addBatch();
                        }
                        pst.executeBatch();
                    }
                }
            } catch (SQLException sqlException) {
                log.error("ob log del 需要顺序处理 /逐个删除 sqlException ", sqlException);
                throw FatalException.of("ob log  del /逐个删除 需要顺序处理 sqlException");
            }
        }
    }


    private void detailSnapShot(List<ArrayList<OceanBaseRecordItem>> batchList) throws ExecutionException, InterruptedException {
        List<ArrayList<JdbcTableValue>> convertSnapShotList = convertSnapShot(batchList);
        if (isAllDifferent(convertSnapShotList)) {
            List<CompletableFuture<Boolean>> futureList = new ArrayList<>(convertSnapShotList.size());
            parallelAddFutureList(convertSnapShotList, futureList);
            log.warn("ob futureList size={}", futureList.size());
            final CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]));
            try {
                voidCompletableFuture.get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                log.error("ob parallel异常，需要人工处理TimeoutException", e);
                throw FatalException.of("ob，需要人工处理TimeoutException");
            }
            log.warn("ob end futureList size={}", futureList.size());
            for (CompletableFuture<Boolean> booleanCompletableFuture : futureList) {
                if (Boolean.FALSE.equals(booleanCompletableFuture.get())) {
                    throw FatalException.of("ob parallel异常,存在处理错误的异常");
                }
            }
        } else {
            // 顺序处理。
            log.info("需要顺序处理");
            detailObCommons(convertSnapShotList);
        }
    }

    private void parallelAddFutureList(List<ArrayList<JdbcTableValue>> convertSnapShotList, List<CompletableFuture<Boolean>> futureList) {
        //可以并行处理
        for (ArrayList<JdbcTableValue> jdbcTableValues : convertSnapShotList) {
            //按照表分组
            final Map<String, List<JdbcTableValue>> collect = jdbcTableValues.stream().collect(Collectors.groupingBy(item -> item.getDb() + "." + item.getTableName()));
            for (Map.Entry<String, List<JdbcTableValue>> entry : collect.entrySet()) {
                //特定一个表的一批
                final List<JdbcTableValue> tmpList = entry.getValue();
                futureList.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        batchReplaceInsert(tmpList);
                    } catch (SQLException sqlException) {
                        log.error("ob parallel jobDetail ds sqlException", sqlException);
                        return Boolean.FALSE;
                    }
                    return Boolean.TRUE;
                }, threadPoolExecutor));
            }
        }
    }

    private void batchReplaceInsert(List<JdbcTableValue> tmpList) throws SQLException {
        if (CollUtil.isEmpty(tmpList)) {
            return;
        }
        //获取replace insert语句
        JdbcTableValue jdbcTableValue = tmpList.get(0);
        final String replaceInsertSqlByRead = JdbcTableValueSqlHelper.getReplaceInsertSqlByRead(jdbcTableValue);
        log.info("replaceInsertSqlByRead={}", replaceInsertSqlByRead);
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement pst = conn.prepareStatement(replaceInsertSqlByRead)) {
                for (JdbcTableValue tableValue : tmpList) {
                    int i = 0;
                    for (JdbcColumnValue column : tableValue.getAfterColumns()) {
                        JdbcConnnUtil.setField(pst, column, column.getValue(), i++);
                    }
                    pst.addBatch();
                }
                pst.executeBatch();
            }
        }
    }

    private boolean isAllDifferent(List<ArrayList<JdbcTableValue>> convertSnapShotList) {
        int size = 0;
        Set<JdbcTableValue> allSet = new HashSet<>();
        for (ArrayList<JdbcTableValue> jdbcTableValues : convertSnapShotList) {
            allSet.addAll(jdbcTableValues);
            size += jdbcTableValues.size();
        }
        return size == allSet.size();
    }

    public List<ArrayList<JdbcTableValue>> convertSnapShot(List<ArrayList<OceanBaseRecordItem>> batchList) {
        List<ArrayList<JdbcTableValue>> list = new ArrayList<>();
        for (ArrayList<OceanBaseRecordItem> items : batchList) {
            ArrayList<JdbcTableValue> tmpList = new ArrayList<>();
            for (OceanBaseRecordItem item : items) {
                JdbcTableValue itemValue = convertSnapShotItem(item);
                tmpList.add(itemValue);
            }
            list.add(tmpList);
        }
        return list;
    }

    private JdbcTableValue convertSnapShotItem(OceanBaseRecordItem item) {
        JdbcTableValue jdbcTableValue = new JdbcTableValue();
        final JdbcTable jdbcTable = item.getJdbcTable();
        jdbcTableValue.setDb(jdbcTable.getDb());
        jdbcTableValue.setTableName(jdbcTable.getTableName());
        final Map<String, Object> jdbcFields = item.getRecord().getJdbcFields();
        List<JdbcColumnValue> afterColumns = new ArrayList<>(jdbcFields.size());
        for (JdbcColumn jdbcColumn : jdbcTable.getColumnList()) {
            final JdbcColumnValue jdbcColumnValue = JdbcColumnValue.getJdbcColumnValue(jdbcColumn);
            final Object o = jdbcFields.get(jdbcColumn.getName());
            jdbcColumnValue.setValue(o);
            afterColumns.add(jdbcColumnValue);
        }
        jdbcTableValue.setAfterColumns(afterColumns);
        jdbcTableValue.makePksByAfter();
        jdbcTableValue.setOp(JdbcTableValue.OP_READ);
        return jdbcTableValue;
    }


    /**
     * 日志类型的转化
     *
     * @param item
     * @return
     */
    private JdbcTableValue convertObLogItem(OceanBaseRecordItem item) {
        JdbcTableValue jdbcTableValue = new JdbcTableValue();
        final JdbcTable jdbcTable = item.getJdbcTable();
        jdbcTableValue.setDb(jdbcTable.getDb());
        jdbcTableValue.setTableName(jdbcTable.getTableName());
        if (DataMessage.Record.Type.DELETE == item.getRecord().getOpt()) {
            jdbcTableValue.setOp(JdbcTableValue.OP_DELETE);
        } else if (DataMessage.Record.Type.INSERT == item.getRecord().getOpt()) {
            jdbcTableValue.setOp(JdbcTableValue.OP_INSERT);
        } else if (DataMessage.Record.Type.UPDATE == item.getRecord().getOpt() ||
                DataMessage.Record.Type.REPLACE == item.getRecord().getOpt()) {
            jdbcTableValue.setOp(JdbcTableValue.OP_UPDATE);
        } else {
            log.warn("未知op，暂不处理，item={}", item);
            return null;
        }
        List<JdbcColumnValue> beforeColumns = new ArrayList<>();
        for (JdbcColumn column : item.getJdbcTable().getColumnList()) {
            JdbcColumnValue value = JdbcColumnValue.getJdbcColumnValue(column);
            value.setValue(ObLogConvert.convertByLogObj(value, item.getRecord().getLogMessageFieldsBefore().get(value.getName())));
            log.debug("logBefore={}", item.getRecord().getLogMessageFieldsBefore());
            beforeColumns.add(value);
        }
        List<JdbcColumnValue> afterColumns = new ArrayList<>();
        for (JdbcColumn column : item.getJdbcTable().getColumnList()) {
            JdbcColumnValue value = JdbcColumnValue.getJdbcColumnValue(column);
            log.debug("logAfter={}", item.getRecord().getLogMessageFieldsAfter());
            value.setValue(ObLogConvert.convertByLogObj(value, item.getRecord().getLogMessageFieldsAfter().get(value.getName())));
            afterColumns.add(value);
        }
        jdbcTableValue.setBeforeColumns(beforeColumns);
        jdbcTableValue.setAfterColumns(afterColumns);
        if (JdbcTableValue.OP_DELETE == jdbcTableValue.getOp()
                || JdbcTableValue.OP_UPDATE == jdbcTableValue.getOp()) {
            //删除和修改，使用before的信息
            jdbcTableValue.makePksByBefore();
        } else {
            //插入，镜像的 使用 after的信息
            jdbcTableValue.makePksByAfter();
        }
        return jdbcTableValue;
    }

    private boolean isAllSnapShot(List<ArrayList<OceanBaseRecordItem>> batchList) {
        boolean flag = true;
        for (ArrayList<OceanBaseRecordItem> items : batchList) {
            if (!isAllSnapShotList(items)) {
                flag = false;
                break;
            }
        }
        return flag;
    }


    private boolean isAllSnapShotList(List<OceanBaseRecordItem> items) {
        boolean flag = true;
        for (OceanBaseRecordItem item : items) {
            if (!item.getRecord().isSnapshotRecord()) {
                flag = false;
                break;
            }
        }
        return flag;
    }

    /**
     * 批量添加
     *
     * @param batch
     * @throws InterruptedException
     */
    public void addCopy(List<OceanBaseRecordItem> batch) throws InterruptedException {
        final ArrayList<OceanBaseRecordItem> recordItems = CollUtil.newArrayList(batch);
        buffer.put(recordItems);
    }

    /**
     * 关闭
     */
    public void close() {
        if (!closed) {
            closed = true;
            if (buffer.getSize() > 0) {
                log.warn("关闭的时候依旧还有数据没有处理完，ds batchBuffer Process getBuffer()={}", buffer);
            }
            threadPoolExecutor.shutdown();
            dataSource.close();
        }
    }
}
