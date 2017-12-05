package com.wyu.transformer.mr.inbound;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.StatsInboundDimension;
import com.wyu.transformer.model.value.map.TextsOutputValue;
import com.wyu.transformer.model.value.reduce.InboundReducerValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ken
 * @date 2017/12/5
 */
public class InboundReducer extends Reducer<StatsInboundDimension,TextsOutputValue,StatsInboundDimension,InboundReducerValue> {
    private Set<String> uvs = new HashSet<String>();
    private Set<String> visits = new HashSet<String>();
    private InboundReducerValue outputValue = new InboundReducerValue();

    @Override
    protected void reduce(StatsInboundDimension key, Iterable<TextsOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            for (TextsOutputValue value : values) {
                this.uvs.add(value.getUuid());
                this.visits.add(value.getSid());
            }

            this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            this.outputValue.setUvs(this.uvs.size());
            this.outputValue.setVisit(this.visits.size());
            context.write(key, this.outputValue);
        } finally {
            // 清空操作
            this.uvs.clear();
            this.visits.clear();
        }
    }
}
