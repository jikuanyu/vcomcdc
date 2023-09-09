package com.zzvcom.cdc.oceanbase;

import com.ververica.cdc.connectors.oceanbase.table.OceanBaseDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

public class OceanBaseDataSchema implements OceanBaseDeserializationSchema<OceanBaseRecord> {


    @Override
    public void deserialize(OceanBaseRecord record, Collector<OceanBaseRecord> out) throws Exception {
        out.collect(record);
    }


    @Override
    public TypeInformation<OceanBaseRecord> getProducedType() {
        return TypeInformation.of(OceanBaseRecord.class);
    }
}
