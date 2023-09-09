package com.zzvcom.cdc.oceanbase.sink;


import com.ververica.cdc.connectors.oceanbase.table.OceanBaseRecord;
import com.zzvcom.cdc.jdbc.JdbcTable;

/**
 * @author yujikuan
 */
public class OceanBaseRecordItem implements java.io.Serializable {

    private static final long serialVersionUID = 3627325826455596493L;
    /**
     * flink cdc 暴露格式的数据
     */

    private OceanBaseRecord record;
    /**
     * 该条数据对应的数据信息
     */
    private JdbcTable jdbcTable;

    public OceanBaseRecord getRecord() {
        return record;
    }

    public void setRecord(OceanBaseRecord record) {
        this.record = record;
    }

    public OceanBaseRecordItem() {

    }

    public OceanBaseRecordItem(OceanBaseRecord record) {
        this.record = record;
    }

    public JdbcTable getJdbcTable() {
        return jdbcTable;
    }

    public void setJdbcTable(JdbcTable jdbcTable) {
        this.jdbcTable = jdbcTable;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OceanBaseRecordItem={");
        if (record != null) {
            OceanBaseRecord.SourceInfo sourceInfo = record.getSourceInfo();
            if (sourceInfo != null) {
                sb.append("sourceInfo={");
                sb.append("tenant=");
                sb.append(sourceInfo.getTenant());
                sb.append(",database=");
                sb.append(sourceInfo.getDatabase());
                sb.append(",table=");
                sb.append(sourceInfo.getTable());
                sb.append(",timestampS=");
                sb.append(sourceInfo.getTimestampS());
                sb.append("},");
            }
            sb.append(",isSnapshotRecord=");
            sb.append(record.isSnapshotRecord());
            sb.append(",jdbcFields=");
            sb.append(record.getJdbcFields());
            sb.append(",opt=");
            sb.append(record.getOpt());
            sb.append(",logMessageFieldsBefore=");
            sb.append(record.getLogMessageFieldsBefore());
            sb.append(",logMessageFieldsAfter=");
            sb.append(record.getLogMessageFieldsAfter());
        }
        sb.append(",jdbcTable==").append(jdbcTable);
        sb.append("}");
        return sb.toString();
    }
}
