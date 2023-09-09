package com.zzvcom.cdc.oceanbase.sink.batch;

import com.ververica.cdc.connectors.oceanbase.table.OceanBaseRecord;
import com.zzvcom.cdc.jdbc.JdbcTable;
import com.zzvcom.cdc.oceanbase.sink.OceanBaseRecordItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author yujikuan
 * @Classname OceanBaseBatchSinkJDBC
 * @Description 批量sink
 * @Date 2023/8/23 15:48
 */
public class OceanBaseBatchSinkJDBC extends RichSinkFunction<OceanBaseRecord> {

    private static final Logger log= LoggerFactory.getLogger(OceanBaseBatchSinkJDBC.class);

    private  final OceanBaseBatchJdbcWriter writer;
    /**
     * 同步库目标的表结构，目前oceanbase log 没有办法获取表结构,所以进行了目标库的获取。
     */
    private final Map<String, JdbcTable> dbTableMap;

    public OceanBaseBatchSinkJDBC(OceanBaseBatchJdbcWriter writer,Map<String, JdbcTable> dbTableMap) {
            this.writer=writer;
            this.dbTableMap=dbTableMap;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        writer.open();
    }

    @Override
    public void close() throws Exception {
        writer.close();
        super.close();
    }

    @Override
    public void invoke(OceanBaseRecord value, Context context) throws Exception {

        OceanBaseRecordItem item = new OceanBaseRecordItem(value);
        //数据结构
        JdbcTable jdbcTable = dbTableMap.get(item.getRecord().getSourceInfo().getDatabase() + "." + item.getRecord().getSourceInfo().getTable());
        if (jdbcTable == null) {
            log.error("没有找到对应的目标表接口，忽略数据,item={}" , item);
            return;
        }
        item.setJdbcTable(jdbcTable);
        writer.addToBatch(item);
    }
}
