package com.zzvcom.cdc.mysql.sink.batch;

import com.zzvcom.cdc.mysql.simple.TableItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author yujikuan
 * @Classname BatchTableSinkJDBC
 * @Description 简单的批量处理。
 * @Date 2023/8/8 17:43
 */
public class BatchTableSinkJdbc extends RichSinkFunction<TableItem> {

    /**
     * jdbc连接器提供者。
     */
    private final BatchJdbcWriter batchJdbcWriter;

    public BatchTableSinkJdbc(BatchJdbcWriter batchJdbcWriter){
        this.batchJdbcWriter=batchJdbcWriter;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        batchJdbcWriter.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        batchJdbcWriter.close();
    }
    @Override
    public void invoke(TableItem value, SinkFunction.Context context) throws Exception {
        batchJdbcWriter.addToBatch(value);
    }



}
