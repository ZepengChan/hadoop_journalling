package com.wyu.transformer.mr.nu;

import com.wyu.commom.DateEnum;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.*;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.mr.TransformerBaseMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 自定义的计算新用户的mapper类
 * @author ken
 */
public class NewInstallUserMapper extends TransformerBaseMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private TimeOutputValue timeOutputValue = new TimeOutputValue();
    private KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    private KpiDimension newInstallUserOfBrowser = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        String uuid = super.getUuid(value);
        String serverTime = super.getServerTime(value);
        String platform = super.getPlatform(value);

        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            logger.warn("uuid & serverTime & platform不能为空!");
            this.filterRecords++;
            return;
        }
        long longOfTime = Long.valueOf(serverTime.trim());
        timeOutputValue.setId(uuid);
        timeOutputValue.setTime(longOfTime);

        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);

        List<PlatFormDimension> platFormDimensionList = PlatFormDimension.buildList(platform);

        /*设置date维度*/

        StatsCommonDimension statsCommonDimension = this.statsUserDimension.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);


        /*写browser相关数据*/

        String browserName = super.getBrowserName(value);
        String browserVersion = super.getBrowserVersion(value);
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);

        for (PlatFormDimension pf : platFormDimensionList) {
            /*清空Browser内容*/
            statsUserDimension.getBrowser().clean();
            statsCommonDimension.setKpi(newInstallUserKpi);
            statsCommonDimension.setPlatForm(pf);
            context.write(statsUserDimension, timeOutputValue);
            this.outputRecords++;
            for (BrowserDimension br : browserDimensionList) {
                statsCommonDimension.setKpi(newInstallUserOfBrowser);
                statsUserDimension.setBrowser(WritableUtils.clone(br,context.getConfiguration()));
                context.write(statsUserDimension, timeOutputValue);
                this.outputRecords++;
            }
        }
    }
}
