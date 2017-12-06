package com.wyu.transformer.mr.inbound.bounce;


import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.StatsCommonDimension;
import com.wyu.transformer.model.dim.StatsInboundBounceDimension;
import com.wyu.transformer.model.dim.StatsInboundDimension;
import com.wyu.transformer.model.value.reduce.InboundBounceReducerValue;
import com.wyu.transformer.service.impl.InboundDimensionService;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 统计外链跳出会话的reducer类
 * 
 * @author ken
 *
 */
public class InboundBounceReducer extends Reducer<StatsInboundBounceDimension, IntWritable, StatsInboundDimension, InboundBounceReducerValue> {
    private StatsInboundDimension statsInboundDimension = new StatsInboundDimension();

    @Override
    protected void reduce(StatsInboundBounceDimension key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String preSid = ""; // 前一条记录的会话id
        String curSid; // 当前会话id
        // 当前inbound id
        int curInboundId;
        // 前一个记录的inbound id
        int preInboundId = InboundBounceMapper.DEFAULT_INBOUND_ID;
        boolean isBounceVisit = true; // true表示会话是一个跳出会话，否则不是跳出会话
        boolean isNewSession = true; // 表示是一个新的会话

        Map<Integer, InboundBounceReducerValue> map = new HashMap<>();
        map.put(InboundDimensionService.ALL_OF_INBOUND_ID, new InboundBounceReducerValue()); // 给一个默认值

        for (IntWritable value : values) {
            curSid = key.getSid();
            curInboundId = value.get();

            // 同一个会话，而且当前inbound id为0，那么一定是一个非跳出的visit
            if (curSid.equals(preSid) && (curInboundId == InboundBounceMapper.DEFAULT_INBOUND_ID)) {
                isBounceVisit = false; // 该会话不是一个跳出会话
                continue;
            }

            // 总的两种情况：
            // 1. 一个新会话
            // 2. 一个非0的inbound id

            // 表示是一个新会话或者是一个新的inbound，检查上一个inbound是否是一个跳出的inbound
            if (preInboundId != InboundBounceMapper.DEFAULT_INBOUND_ID && isBounceVisit) {
                // 针对上一个inbound id需要进行一次跳出会话更新操作
                map.get(preInboundId).incrBounceNum();

                // 针对all维度的bounce number的计算
                // 规则1：一次会话中只要出现一次跳出外链，就将其算到all维度的跳出会话中去，只要出现一次跳出就算做跳出。
                if (!curSid.equals(preSid)) {
                    map.get(InboundDimensionService.ALL_OF_INBOUND_ID).incrBounceNum();
                }
            }

            // 会话结束或者是当前inboundid不为0，而且和前一个inboundid不相等。
            isBounceVisit = true;
            preInboundId = InboundBounceMapper.DEFAULT_INBOUND_ID;

            // 如果inbound是一个新的
            if (curInboundId != InboundBounceMapper.DEFAULT_INBOUND_ID) {
                preInboundId = curInboundId;
                InboundBounceReducerValue irv = map.get(curInboundId);
                if (irv == null) {
                    irv = new InboundBounceReducerValue(0);
                    map.put(curInboundId, irv);
                }
            }

            // 如果是一个新的会话，那么更新会话
            if (!preSid.equals(curSid)) {
                isNewSession = true;
                preSid = curSid;
            } else {
                isNewSession = false;
            }
        }

        // 单独的处理最后一条数据
        // 表示是一个新会话或者是一个新的inbound，检查上一个inbound是否是一个跳出的inbound
        if (preInboundId != InboundBounceMapper.DEFAULT_INBOUND_ID && isBounceVisit) {
            // 针对上一个inbound id需要进行一次跳出会话更新操作
            map.get(preInboundId).incrBounceNum();

            // 针对all维度的bounce number的计算
            // 规则1：一次会话中只要出现一次跳出外链，就将其算到all维度的跳出会话中去，只要出现一次跳出就算做跳出。
            if (isNewSession) {
                map.get(InboundDimensionService.ALL_OF_INBOUND_ID).incrBounceNum();
            }
        }

        // 数据输出
        this.statsInboundDimension.setStatsCommon(StatsCommonDimension.clone(key.getStatsCommon()));
        for (Map.Entry<Integer, InboundBounceReducerValue> entry : map.entrySet()) {
            InboundBounceReducerValue value = entry.getValue();
            value.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            this.statsInboundDimension.getInbound().setId(entry.getKey());
            context.write(this.statsInboundDimension, value);
        }
    }
}
