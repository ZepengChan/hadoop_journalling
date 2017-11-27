package com.wyu.transformer.mr.nm;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 统计new member reducer类
 * @author ken
 * @date 2017/11/27
 */
public class NewMemberReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension,MapWritableValue> {

    private static final Logger logger = Logger.getLogger(NewMemberReducer.class);
    private Set<String> unique = new HashSet<>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            //统计member id  去重
            for (TimeOutputValue value : values) {
                this.unique.add(value.getId());
            }

            this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
            this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            this.outputValue.setValue(this.map);
            context.write(key, outputValue);
        } finally {
            //清空
            unique.clear();
        }
    }
}
