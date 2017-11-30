package com.wyu.transformer.mr.pv;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**统计PV 的reducer类
 * 不涉及去重,直接统计输出到reducer key中的,value的个数
 * @author ken
 * @date 2017/11/30
 */
public class PageViewReducer extends Reducer<StatsUserDimension, NullWritable, StatsUserDimension, MapWritableValue> {

    private MapWritableValue mapWritableValue = new MapWritableValue();
    private MapWritable map = new MapWritable();



	@Override
    protected void reduce(StatsUserDimension key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int pvCount = 0 ;
        for(NullWritable ignored : values){
            //每一条直接 +1 不过滤
            pvCount++;
        }

        this.map.put(new IntWritable(-1),new IntWritable(pvCount));
        this.mapWritableValue.setValue(map);

        //填充kpi
        this.mapWritableValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));

        //s输出
        context.write(key,this.mapWritableValue);
    }
}
