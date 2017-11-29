package com.wyu.transformer.mr.au;

import com.wyu.commom.DateEnum;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 统计active user,计算一组中uuid的个数
 *
 * @author ken
 * @date 2017/11/26
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    private Set<String> unique = new HashSet<>();
    private Map<Integer, Set<String>> hourlyUnique = new HashMap<>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();
    private MapWritable hourlyMap = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        for (int i = 0; i < 24; i++) {
            //初始化24小时的数据
            this.hourlyMap.put(new IntWritable(i), new IntWritable(0));
            this.hourlyUnique.put(i, new HashSet<String>());
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            String kpiName = key.getStatsCommon().getKpi().getKpiName();
            if (KpiType.HOURLY_ACTIVE_USER.name.equals(kpiName)) {
                //计算hourly active user
                for (TimeOutputValue value : values) {
                    //计算访问的小时  [0,23]
                    int hour = TimeUtil.getDateInfo(value.getTime(), DateEnum.HOUR);
                    this.hourlyUnique.get(hour).add(value.getId());
                }
                this.outputValue.setKpi(KpiType.HOURLY_ACTIVE_USER);
                for (Map.Entry<Integer, Set<String>> entry : this.hourlyUnique.entrySet()) {
                    this.hourlyMap.put(new IntWritable(entry.getKey()),new IntWritable(entry.getValue().size()));
                }
                this.outputValue.setValue(this.hourlyMap);

                //输出
                context.write(key,this.outputValue);
            } else {
                //统计uuid  去重
                for (TimeOutputValue value : values) {
                    this.unique.add(value.getId());
                }

                this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
                this.map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
                this.outputValue.setValue(this.map);
                context.write(key, outputValue);
            }
        } finally {
            //清空
            unique.clear();
            this.map.clear();
            this.hourlyMap.clear();
            this.hourlyUnique.clear();
            for (int i = 0; i < 24; i++) {
                //初始化24小时的数据
                this.hourlyMap.put(new IntWritable(i), new IntWritable(0));
                this.hourlyUnique.put(i, new HashSet<String>());
            }
        }
    }
}
