package com.zzvcom.cdc.mysql;


import cn.hutool.core.collection.CollUtil;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.zzvcom.cdc.mysql.simple.ColumnItem;
import com.zzvcom.cdc.mysql.simple.TableItem;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * @author yujikuan
 */
public class TableItemDeserializationSchema implements DebeziumDeserializationSchema<TableItem> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(TableItemDeserializationSchema.class);

    @Override
    public TypeInformation<TableItem> getProducedType() {
        return TypeInformation.of(TableItem.class);
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<TableItem> out) throws Exception {

        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStruct = valueStruct.getStruct("source");
        //获取数据库名称,表名,操作类型
        String database = sourceStruct.getString("db");
        String table = sourceStruct.getString("table");
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        TableItem tableItem = new TableItem();
        tableItem.setTable(table);
        tableItem.setDb(database);
        tableItem.setOp(operation);
        tableItem.setBefore(extracted(valueStruct, "before"));
        //格式转换
        tableItem.setAfter(extracted(valueStruct, "after"));
        Struct keyStruct = (Struct) sourceRecord.key();
        Schema schema = keyStruct.schema();
        List<ColumnItem> pks = new ArrayList<>();
        for (Field field : schema.fields()) {
            String name = field.name();
            Object o = keyStruct.get(name);
            ColumnItem columnItem = new ColumnItem();

            columnItem.setType(field.schema().type());
            columnItem.setSchemaName(field.schema().name());
            columnItem.setValue(o);
            columnItem.setName(name);
            pks.add(columnItem);
        }
        if (CollUtil.isEmpty(pks)) {
            log.warn("没有主键不支持处理,tableItem={}", tableItem);
            return;
        }
        tableItem.setPks(pks);
        out.collect(tableItem);
    }

    private List<ColumnItem> extracted(Struct valueStruct, String beforeOrAfter) {
        Struct beforeStruct = valueStruct.getStruct(beforeOrAfter);
        if (beforeStruct != null) {
            List<ColumnItem> beforeList = new ArrayList<>();
            for (Field field : beforeStruct.schema().fields()) {
                ColumnItem columnItem = new ColumnItem();
                columnItem.setName(field.name());
                columnItem.setValue(beforeStruct.get(field));
                Schema schema = field.schema();
                columnItem.setType(schema.type());
                columnItem.setSchemaName(schema.name());
                beforeList.add(columnItem);
            }
            return beforeList;
        }
        return Collections.emptyList();
    }


}
