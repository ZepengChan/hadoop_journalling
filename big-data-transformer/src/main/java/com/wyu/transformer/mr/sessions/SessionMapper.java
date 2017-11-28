package com.wyu.transformer.mr.sessions;

import com.wyu.commom.DateEnum;
import com.wyu.commom.EventLogConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.*;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 统计会话个数&会话长度mapper类
 * @author ken
 * @date 2017/11/28
 */
public class SessionMapper extends TableMapper<StatsUserDimension,TimeOutputValue>{

    private static final Logger logger = Logger.getLogger(SessionMapper.class);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension sessionKpi = new KpiDimension(KpiType.SESSIONS.name);
    private KpiDimension sessionBrowserKpi = new KpiDimension(KpiType.BROWSER_SESSIONS.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //获取会话id serverTime platform
        String sessionId = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PALTFORM)));

        if (StringUtils.isBlank(sessionId) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            logger.warn("sessionId & serverTime & platform不能为空!");
            return;
        }
        long longOfTime = Long.valueOf(serverTime.trim());
        outputValue.setId(sessionId);
        outputValue.setTime(longOfTime);

        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);

        List<PlatFormDimension> platForms = PlatFormDimension.buildList(platform);

           /*设置date维度*/
        StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);

         /*写browser相关数据*/
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);

        for (PlatFormDimension pf : platForms) {
            this.outputKey.setBrowser(defaultBrowser);
            //设置platform
            statsCommonDimension.setPlatForm(pf);
            statsCommonDimension.setKpi(sessionKpi);
            //设置kpi
            context.write(this.outputKey, this.outputValue);
            //browser 维度统计
            statsCommonDimension.setKpi(sessionBrowserKpi);
            for (BrowserDimension br : browserDimensionList) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
            }
        }
    }
}
