package com.wyu.transformer.mr.au;

import com.wyu.commom.DateEnum;
import com.wyu.commom.KpiType;
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
 * active user 的mapper类
 *
 * @author ken
 * @date 2017/11/26
 */
public class ActiveUserMapper extends TransformerBaseMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.name);
    private KpiDimension activeUserBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.name);
    private KpiDimension hourlyActiveUserKpi = new KpiDimension(KpiType.HOURLY_ACTIVE_USER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        //获取uuid等数据
        String uuid = super.getUuid(value);
        String platform = super.getPlatform(value);
        String serverTime = super.getServerTime(value);
        //过滤无效数据
        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            System.out.println("uuid:" + uuid
                    + "\tplatform:" + platform
                    + "\tserverTime:" + serverTime);
            logger.warn("uuid & serverTime & platform不能为空!");
            this.filterRecords++;
            return;
        }
        //获取日期相关信息,其中id是uuid
        long longOfTime = Long.valueOf(serverTime.trim());
        this.outputValue.setTime(longOfTime);
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
        this.outputValue.setId(uuid);

        //创建platform
        List<PlatFormDimension> platForms = PlatFormDimension.buildList(platform);

        //获取浏览器相关信息
        String browserName = super.getBrowserName(value);
        String browserVersion = super.getBrowserVersion(value);
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);
        //开始输出
        StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
        //设置date
        statsCommonDimension.setDate(dateDimension);
        for (PlatFormDimension pf : platForms) {
            this.outputKey.setBrowser(defaultBrowser);
            //设置platform
            statsCommonDimension.setPlatForm(pf);

            //输出active user的键值对
            //设置kpi
            statsCommonDimension.setKpi(activeUserKpi);
            context.write(this.outputKey, this.outputValue);

            //输出hourly active user键值对
            statsCommonDimension.setKpi(hourlyActiveUserKpi);
            context.write(this.outputKey,this.outputValue);
            this.outputRecords++;
            //browser 维度统计
            statsCommonDimension.setKpi(activeUserBrowserKpi);
            for (BrowserDimension br : browserDimensionList) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
                this.outputRecords++;
            }
        }
    }
}
