package com.wyu.transformer.mr.am;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 统计active member,计算一组中mid的个数
 *
 * @author ken
 * @date 2017/11/26
 */
public class ActiveMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    private Set<String> unique = new HashSet<>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            //统计uuid  去重
            for (TimeOutputValue value : values) {
                this.unique.add(value.getId());
            }

            this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
            this.outputValue.setValue(this.map);
            context.write(key, outputValue);
        } finally {
            //清空
            unique.clear();
        }
    }
}
