package com.wyu.transformer.mr.sessions;

import com.wyu.commom.GlobalConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 统计会话个数&会话长度reducer类
 *
 * @author ken
 * @date 2017/11/28
 */
public class SessionReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    private Map<String, TimeChain> timeChainMap = new HashMap<>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            //统计session id  去重
            for (TimeOutputValue value : values) {
                TimeChain chain = this.timeChainMap.get(value.getId());
                if (chain == null) {
                    chain = new TimeChain(value.getTime());
                    //保存
                    this.timeChainMap.put(value.getId(), chain);
                }
                chain.addTime(value.getTime());
            }
            int sessionLength = 0;
            //计算间隔毫秒数(session时长)
            for (Map.Entry<String, TimeChain> entry : this.timeChainMap.entrySet()) {
                //间隔毫秒数
                long tmp = entry.getValue().getTimeOfmillis();
                if (tmp < 0 || tmp > GlobalConstants.DAY_OF_MILLISECONDS) {
                    //如果间隔毫秒数小于0或者大于一天的毫秒数,直接过滤
                    continue;
                }
                sessionLength += tmp;
            }
            //获取秒数
            if(sessionLength % 1000 == 0){
                sessionLength = sessionLength / 1000 ;
            }else{
                sessionLength = sessionLength / 1000 + 1;
            }

            this.map.put(new IntWritable(-1), new IntWritable(this.timeChainMap.size()));
            this.map.put(new IntWritable(-2), new IntWritable(sessionLength));
            this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            this.outputValue.setValue(this.map);
            context.write(key, outputValue);
        } finally {
            //清空
        }
    }

}
