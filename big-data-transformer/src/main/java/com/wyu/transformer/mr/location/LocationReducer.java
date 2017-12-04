package com.wyu.transformer.mr.location;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.StatsLocationDimension;
import com.wyu.transformer.model.value.map.TextsOutputValue;
import com.wyu.transformer.model.value.reduce.LocationReducerOutputValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**统计地域信息 reducer类
 * @author ken
 * @date 2017/12/4
 */
public class LocationReducer extends Reducer<StatsLocationDimension,TextsOutputValue,StatsLocationDimension,LocationReducerOutputValue> {
    private Set<String> uvs = new HashSet<String>();
    private Map<String, Integer> sessions = new HashMap<String, Integer>();
    private LocationReducerOutputValue outputValue = new LocationReducerOutputValue();

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<TextsOutputValue> values, Context context) throws IOException, InterruptedException {
       try{
           for(TextsOutputValue value : values){
               String uuid = value.getUuid();
               String sid = value.getSid();

               //去重添加uuid
               this.uvs.add(uuid);

               //分别标识sid,已有访问数据标志为2,访问一次标识为1
               if(this.sessions.containsKey(sid)){
                   this.sessions.put(sid,2);
               }else{
                   this.sessions.put(sid,1);
               }
           }

           //设置输出对象
           this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
           this.outputValue.setUvs(this.uvs.size());
           this.outputValue.setVisits(this.sessions.size());
           int bounceNumber = 0;
           for(Map.Entry<String,Integer> entry : sessions.entrySet()){
               if(entry.getValue() == 1){
                   bounceNumber++;
               }
           }
           this.outputValue.setBounceNumber(bounceNumber);

           context.write(key,this.outputValue);

       }finally {
           this.uvs.clear();
           this.sessions.clear();
       }

    }
}
