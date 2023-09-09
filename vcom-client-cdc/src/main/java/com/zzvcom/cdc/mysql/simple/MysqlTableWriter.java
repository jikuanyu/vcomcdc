package com.zzvcom.cdc.mysql.simple;

import io.debezium.data.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import static com.zzvcom.cdc.mysql.simple.TableItemSqlHelper.setPst;

/**
 * 效率低不采用
 */
public class MysqlTableWriter {

    private static final Logger log = LoggerFactory.getLogger(MysqlTableWriter.class);

    private MysqlTableWriter() {
    }

    public static void excSql(TableItem tableItem, Connection connection) throws SQLException {
        log.debug("tableItem={}", tableItem);
        if (Envelope.Operation.CREATE == tableItem.getOp()) {
            insert(tableItem, connection);
        } else if (Envelope.Operation.READ == tableItem.getOp()) {
            read(tableItem, connection);
        } else if (Envelope.Operation.UPDATE == tableItem.getOp()) {
            String findSql = TableItemSqlHelper.getFindSql(tableItem);
            if (findSql == null) {
                return;
            }
            if (isExist(tableItem, connection, findSql)) {
                update(tableItem, connection);
            } else {
                insert(tableItem, connection);
            }

        } else if (Envelope.Operation.DELETE == tableItem.getOp()) {
            delete(tableItem, connection);
        } else if (Envelope.Operation.TRUNCATE == tableItem.getOp()) {
            log.debug("TRUNCATE 暂不处理 ={}", tableItem);
        } else {
            log.debug("MESSAGE={}", tableItem);
        }
    }

    private static void read(TableItem tableItem, Connection connection) throws SQLException {
        log.debug("READ={}", tableItem);
        String findSql = TableItemSqlHelper.getFindSql(tableItem);
        if (findSql != null) {
            if (isExist(tableItem, connection, findSql)) {
                update(tableItem, connection);
            } else {
                insert(tableItem, connection);
            }
        } else {
            //没有主键，支持全量同步init的时候
            insert(tableItem, connection);
        }
    }


    private static void delete(TableItem tableItem, Connection connection) throws SQLException {
        String deleteSql = TableItemSqlHelper.getDeleteSql(tableItem);
        log.debug("deleteSql={}", deleteSql);
        if (deleteSql == null) {
            return;
        }
        try (PreparedStatement pst = connection.prepareStatement(deleteSql)) {
            int i = 1;
            for (ColumnItem pk : tableItem.getPks()) {
                setPst(pst, i, pk);
                i++;
            }
            pst.executeUpdate();
        }
    }

    private static void update(TableItem tableItem, Connection connection) throws SQLException {
        //存在更新
        String updateSql = TableItemSqlHelper.getUpdateSql(tableItem);
        if (updateSql == null) {
            return;
        }
        log.debug("updateSql={}", updateSql);
        try (PreparedStatement pst = connection.prepareStatement(updateSql)) {
            TableItemSqlHelper.pstUpdate(tableItem, pst);
            pst.executeUpdate();
        }
    }


    private static boolean isExist(TableItem tableItem, Connection connection, String findSql) {
        PreparedStatement pst = null;
        boolean flag = false;
        try {
            pst = connection.prepareStatement(findSql);
            int i = 1;
            for (ColumnItem pk : tableItem.getPks()) {
                setPst(pst, i, pk);
                i++;
            }
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                rs.close();
                flag = true;
            }
        } catch (Exception throwables) {
            log.error("isExist ex", throwables);
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
            } catch (SQLException throwables) {
                log.error("isExistclose", throwables);
            }
        }
        return flag;
    }

    private static void insert(TableItem tableItem, Connection connection) {
        String insertSql = TableItemSqlHelper.getInsertSql(tableItem);
        log.debug("insertSql={}", insertSql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {
            int i = 1;
            for (ColumnItem columnItem : tableItem.getAfter()) {
                setPst(preparedStatement, i, columnItem);
                i++;
            }
            final int i1 = preparedStatement.executeUpdate();
            log.debug("i1={}", i1);
        } catch (Exception ex) {
            log.error("insertSqlfail ", ex);
        }
    }
}
