package com.wyu.transformer.mr.am;

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
 * active member 的mapper类
 *
 * @author ken
 * @date 2017/11/26
 */
public class ActiveMemberMapper extends TransformerBaseMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVER_MEMBER.name);
    private KpiDimension activeMemberBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        //获取u_Mid等数据
        String memberId = super.getMemberId(value);
        String platform = super.getPlatform(value);
        String serverTime = super.getServerTime(value);
        //过滤无效数据
        if (StringUtils.isBlank(memberId) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            System.out.println("memberId:" + memberId
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
        this.outputValue.setId(memberId);

        //创建platform
        List<PlatformDimension> platForms = PlatformDimension.buildList(platform);

        //获取浏览器相关信息
        String browserName = super.getBrowserName(value);
        String browserVersion = super.getBrowserVersion(value);
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);
        //开始输出
        StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
        //设置date
        statsCommonDimension.setDate(dateDimension);
        for (PlatformDimension pf : platForms) {
            this.outputKey.setBrowser(defaultBrowser);

            //设置kpi
            statsCommonDimension.setKpi(activeMemberKpi);
            //设置platform
            statsCommonDimension.setPlatform(pf);
            context.write(this.outputKey, this.outputValue);
            this.outputRecords++;

            //browser 维度统计
            statsCommonDimension.setKpi(activeMemberBrowserKpi);
            for (BrowserDimension br : browserDimensionList) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
                this.outputRecords++;
            }
        }
    }
}
