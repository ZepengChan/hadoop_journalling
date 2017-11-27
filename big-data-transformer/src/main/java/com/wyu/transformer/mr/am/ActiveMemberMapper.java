package com.wyu.transformer.mr.am;

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
 * active member 的mapper类
 *
 * @author ken
 * @date 2017/11/26
 */
public class ActiveMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    public static byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVER_MEMBER.name);
    private KpiDimension activeMemberBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取u_Mid等数据
        String memberId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PALTFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        //过滤无效数据
        if (StringUtils.isBlank(memberId) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            System.out.println("memberId:" + memberId
                    + "\tplatform:" + platform
                    + "\tserverTime:" + serverTime);
            logger.warn("uuid & serverTime & platform不能为空!");
            return;
        }
        //获取日期相关信息,其中id是uuid
        long longOfTime = Long.valueOf(serverTime.trim());
        this.outputValue.setTime(longOfTime);
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
        this.outputValue.setId(memberId);

        //创建platform
        List<PlatFormDimension> platForms = PlatFormDimension.buildList(platform);

        //获取浏览器相关信息
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);
        //开始输出
        StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
        //设置date
        statsCommonDimension.setDate(dateDimension);
        for (PlatFormDimension pf : platForms) {
            this.outputKey.setBrowser(defaultBrowser);
            //设置platform
            statsCommonDimension.setPlatForm(pf);
            //设置kpi
            statsCommonDimension.setKpi(activeMemberKpi);
            context.write(this.outputKey, this.outputValue);

            //browser 维度统计
            statsCommonDimension.setKpi(activeMemberBrowserKpi);
            for (BrowserDimension br : browserDimensionList) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
            }
        }
    }
}
