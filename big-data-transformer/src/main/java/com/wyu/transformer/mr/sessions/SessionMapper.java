package com.wyu.transformer.mr.sessions;

import com.wyu.commom.DateEnum;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.StatsCommonDimension;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.dim.base.*;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.mr.TransformerBaseMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 统计会话个数&会话长度mapper类
 * @author ken
 * @date 2017/11/28
 */
public class SessionMapper extends TransformerBaseMapper<StatsUserDimension,TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(SessionMapper.class);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension sessionKpi = new KpiDimension(KpiType.SESSIONS.name);
    private KpiDimension sessionBrowserKpi = new KpiDimension(KpiType.BROWSER_SESSIONS.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        //获取会话id serverTime platform
        String sessionId = super.getSessionId(value);
        String serverTime = super.getServerTime(value);
        String platform = super.getPlatform(value);

        if (StringUtils.isBlank(sessionId) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            logger.warn("sessionId & serverTime & platform不能为空!");
            this.filterRecords++;
            return;
        }
        long longOfTime = Long.valueOf(serverTime.trim());
        outputValue.setId(sessionId);
        outputValue.setTime(longOfTime);

        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);

        List<PlatformDimension> platForms = PlatformDimension.buildList(platform);

           /*设置date维度*/
        StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);

         /*写browser相关数据*/
        String browserName = super.getBrowserName(value);
        String browserVersion = super.getBrowserVersion(value);
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);

        for (PlatformDimension pf : platForms) {
            this.outputKey.setBrowser(defaultBrowser);
            //设置platform
            statsCommonDimension.setPlatform(pf);
            statsCommonDimension.setKpi(sessionKpi);
            //设置kpi
            context.write(this.outputKey, this.outputValue);
            this.outputRecords++;
            //browser 维度统计
            statsCommonDimension.setKpi(sessionBrowserKpi);
            for (BrowserDimension br : browserDimensionList) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
                this.outputRecords++;
            }
        }
    }
}
