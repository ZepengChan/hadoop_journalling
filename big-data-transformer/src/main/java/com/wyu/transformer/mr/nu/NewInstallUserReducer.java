package com.wyu.transformer.mr.nu;

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
 * @author ken
 * @date 2017/11/23
 */
public class NewInstallUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    private static final Logger logger = Logger.getLogger(NewInstallUserReducer.class);
    private MapWritableValue outputValue = new MapWritableValue();
    private Set<String> unique = new HashSet<>(16);

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        this.unique.clear();
        for(TimeOutputValue value : values){
            this.unique.add(value.getId());
        }
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        outputValue.setValue(map);
        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        if (KpiType.NEW_INSTALL_USER.name.equals(kpiName)) {
            /*计算stats_user表中的新增用户*/
            outputValue.setKpi(KpiType.NEW_INSTALL_USER);
        } else if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(kpiName)) {
            /*计算stats_device_browser表中的新增用户*/
            outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        }

        context.write(key,outputValue);

    }
}
