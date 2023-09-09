package com.zzvcom.cdc.mysql.sink.batch;

import cn.hutool.core.collection.CollUtil;
import com.zzvcom.cdc.mysql.simple.TableItem;
import com.zzvcom.cdc.mysql.simple.TableItemSqlHelper;
import com.zzvcom.cdc.util.conn.VcomJdbcConnectionProvider;
import io.debezium.data.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yujikuan
 * @Classname BatchTools
 * @Description 插入处理工具
 * @Date 2023/8/14 16:22
 */
public class BatchTools implements java.io.Serializable {

    private static final long serialVersionUID = -9188259964717347732L;
    private static Logger log = LoggerFactory.getLogger(BatchTools.class);
    private final List<TableItem> batch;
    private final VcomJdbcConnectionProvider connectionProvider;
    private int maxTryNum;

    public BatchTools(List<TableItem> batch, VcomJdbcConnectionProvider connectionProvider, int maxTryNum) {
        this.batch = batch;
        this.connectionProvider = connectionProvider;
        this.maxTryNum = maxTryNum;
    }

    /**
     * 刷新
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        for (int i = 0; i <= maxTryNum; i++) {
            try {
                log.info("i={},jdbcinfo={},batchSize={}", i, connectionProvider.getJdbcInfo(), batch.size());
                attemptFlush(connectionProvider.getConnection(), batch);
                batch.clear();
                break;
            } catch (SQLException e) {
                log.error("重试JDBC executeBatch error, retry times = {}", i, e);
                if (i >= maxTryNum) {
                    throw new IOException(e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        //重新创建连接
                        connectionProvider.reestablishConnection();
                    }
                } catch (Exception exception) {
                    log.error(
                            "JDBC connection is not valid, and reestablish connection failed.",
                            exception);
                }
                try {
                    Thread.sleep(500L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    /**
     * 尝试刷新
     */
    public static void attemptFlush(Connection connection, final List<TableItem> list) throws SQLException {
        if (list.isEmpty()) {
            return;
        }
        //按照库名
        final Map<String, List<TableItem>> dbTableMap = list.stream().collect(Collectors.groupingBy(item -> (item.getDb() + "." + item.getTable()), Collectors.toList()));
        for (Map.Entry<String, List<TableItem>> item : dbTableMap.entrySet()) {
            final List<TableItem> tables = item.getValue();
            final Map<Envelope.Operation, List<TableItem>> collect = tables.stream().collect(Collectors.groupingBy(TableItem::getOp));
            //挑选 read create update的执行
            serverInsertOrUpdate(collect.get(Envelope.Operation.READ), connection);
            serverInsertOrUpdate(collect.get(Envelope.Operation.CREATE), connection);
            serverInsertOrUpdate(collect.get(Envelope.Operation.UPDATE), connection);
            //刪除的放到最后
            List<TableItem> deleteEntryList = collect.get(Envelope.Operation.DELETE);
            delete(connection, deleteEntryList);
        }
    }

    private static void delete(Connection connection, List<TableItem> deleteEntryList) throws SQLException {
        if (CollUtil.isEmpty(deleteEntryList)) {
            return;
        }
        final TableItem tableItem = deleteEntryList.get(0);
        if (tableItem.getPks() == null) {
            log.error("没有主键不处理，tableItem={}", tableItem);
            return;
        }
        if (tableItem.getPks().size() == 1) {
            try (final Statement st = connection.createStatement()) {
                final String batchDeleteSql = TableItemSqlHelper.getBatchDeleteSql(deleteEntryList);
                log.debug("batchDeleteSql={}", batchDeleteSql);
                st.addBatch(batchDeleteSql);
                st.executeBatch();
            }

        } else {
            final String deleteSql = TableItemSqlHelper.getDeleteSql(tableItem);
            log.debug("deleteSql={}", deleteSql);
            if (deleteSql == null) {
                return;
            }
            try (PreparedStatement pst = connection.prepareStatement(deleteSql)) {
                for (TableItem ti : deleteEntryList) {
                    TableItemSqlHelper.pstPK(ti, pst);
                    pst.addBatch();
                }
                pst.executeBatch();
            }
        }

    }

    private static void serverInsertOrUpdate(List<TableItem> list, Connection connection) throws SQLException {
        if (CollUtil.isEmpty(list)) {
            return;
        }
        final TableItem tableItem = list.get(0);
        final String insertOrUpdateSql = TableItemSqlHelper.insertOrUpdateSql(tableItem);
        log.info("insertOrUpdateSql={}", insertOrUpdateSql);
        try (PreparedStatement pst = connection.prepareStatement(insertOrUpdateSql)) {
            for (TableItem ti : list) {
                TableItemSqlHelper.pstInsert(ti, pst);
                pst.addBatch();
            }
            pst.executeBatch();
        }
        log.debug("batch udate before");
    }

    public void clientInsertOrUpdate(Map.Entry<Envelope.Operation, List<TableItem>> entry) throws SQLException {
        final TableItem tableItem = entry.getValue().get(0);
        final String findSql = TableItemSqlHelper.getFindSql(tableItem);
        //判断是否存在
        try (final PreparedStatement selectPst = connectionProvider.getConnection().prepareStatement(findSql)) {
            for (TableItem ti : entry.getValue()) {
                TableItemSqlHelper.pstPK(ti, selectPst);
                try (final ResultSet resultSet = selectPst.executeQuery()) {
                    if (resultSet.next()) {
                        ti.setExist(true);
                    }
                }
            }
        }
        final Map<Boolean, List<TableItem>> listMap = entry.getValue().stream().collect(Collectors.groupingBy(TableItem::isExist));
        final List<TableItem> updateList = listMap.get(true);
        if (CollUtil.isNotEmpty(updateList)) {
            final String updateSql = TableItemSqlHelper.getUpdateSql(tableItem);
            log.info("updateSql={}", updateSql);
            try (PreparedStatement pst = connectionProvider.getConnection().prepareStatement(updateSql)) {
                for (TableItem ti : updateList) {
                    TableItemSqlHelper.pstUpdate(ti, pst);
                    pst.addBatch();
                }
                log.debug("batch udate before");
                pst.executeBatch();
            }
            log.debug("batch update after");

        }
        final List<TableItem> addList = listMap.get(false);
        if (CollUtil.isNotEmpty(addList)) {
            final String insertSql = TableItemSqlHelper.getInsertSql(tableItem);
            try (PreparedStatement pst = connectionProvider.getConnection().prepareStatement(insertSql)) {
                for (TableItem ti : addList) {
                    TableItemSqlHelper.pstInsert(ti, pst);
                    pst.addBatch();
                }
                pst.executeBatch();
            }
        }
    }

}
