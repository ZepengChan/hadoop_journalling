package com.wyu.transformer.mr.sessions;

import com.wyu.commom.DateEnum;
import com.wyu.commom.GlobalConstants;
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
    private Map<Integer,Map<String,TimeChain>> hourlyTimeChainMap = new HashMap<>();
    private MapWritable hourlySessionsMap = new MapWritable();
    private MapWritable hourlySessionsLengthMap = new MapWritable();



    /**
     * 数据初始化操作
     */
    private void startUp(){
        this.map.clear();
        this.hourlySessionsMap.clear();
        this.timeChainMap.clear();
        this.hourlyTimeChainMap.clear();
        for (int i = 0; i < 24; i++) {
            this.hourlySessionsMap.put(new IntWritable(i),new IntWritable(0));
            this.hourlyTimeChainMap.put(i,new HashMap<String, TimeChain>());
        }
    }
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        this.startUp();
        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        if(KpiType.SESSIONS.name.equals(kpiName)){
            //计算stats_user表中的sessions和sessions_length;同事也计算hourly_sessions和sessions_length
            this.handleSessions(key,values,context);
        }else{
            //处理browser维度的统计信息
            this.handleBrowserSessions(key,values,context);
        }


    }

    /**
     * 处理普通的session
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private void handleSessions(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //统计session id  去重
        for (TimeOutputValue value : values) {
            String sid = value.getId();
            long time = value.getTime();

            //正常处理
            TimeChain chain = this.timeChainMap.get(value.getId());
            if (chain == null) {
                chain = new TimeChain(value.getTime());
                //保存
                this.timeChainMap.put(value.getId(), chain);
            }
            chain.addTime(value.getTime());

            //处理hourly统计
            int hour = TimeUtil.getDateInfo(time, DateEnum.HOUR);
            Map<String,TimeChain> htcm = this.hourlyTimeChainMap.get(hour);
            TimeChain hourlyChain = htcm.get(sid);
            if(hourlyChain == null){
                hourlyChain = new TimeChain(time);
                htcm.put(sid,hourlyChain);
                this.hourlyTimeChainMap.put(hour,htcm);
            }
            //更新时间
            hourlyChain.addTime(time);
        }
        //计算hourly统计信息
        for (Map.Entry<Integer,Map<String,TimeChain>> entry : this.hourlyTimeChainMap.entrySet()){
            //当前小时session的个数
            this.hourlySessionsMap.put(new IntWritable(entry.getKey()),new IntWritable(entry.getValue().size()));
            //统计每小时的会话时长
            int presl = 0;
            for(Map.Entry<String,TimeChain> entry2 :entry.getValue().entrySet()){
                //间隔毫秒数
                long tmp = entry2.getValue().getTimeOfmillis();
                if(tmp < 0 || tmp >= 3600000){
                    //会话时长小于0,或者大于一个小时,直接过滤
                    continue;
                }
                presl += tmp;
            }
            if(presl % 1000 == 0){
                presl = presl / 1000 ;
            }else{
                presl = presl / 1000 + 1;
            }
            this.hourlySessionsLengthMap.put(new IntWritable(entry.getKey()),new IntWritable(presl));
        }

        //进行hourly SESSIONS输出
        this.outputValue.setKpi(KpiType.HOURLY_SESSIONS);
        this.outputValue.setValue(this.hourlySessionsMap);
        context.write(key, outputValue);

        //进行hourly session length输出
        this.outputValue.setKpi(KpiType.HOURLY_SESSIONS_LENGTH);
        this.outputValue.setValue(this.hourlySessionsLengthMap);
        context.write(key,outputValue);

        //计算正常的session和session length
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
        //获取秒数,不足1秒的，按1 秒计算
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
        //清空
    }

    /**
     * 处理带有browser维度的sessions
     *
     * @param key
     * @param values
     * @param context
     */
    private void handleBrowserSessions(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
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
        //获取秒数,不足1秒的，按1 秒计算
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
        //清空
    }

}
