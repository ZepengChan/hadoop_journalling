package com.wyu.transformer.mr.pv;

import com.wyu.commom.DateEnum;
import com.wyu.commom.EventLogConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**统计pv mapper类
 * 输入hbase数据,包括platform,serverTime,browserName,BrowserVersion,url
 * 输出<StatsUserDimension,NullWritable>键值对
 * @author ken
 * @date 2017/11/30
 */
public class PageViewMapper extends TableMapper<StatsUserDimension,NullWritable> {
    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private  KpiDimension websitePageViewDimension = new KpiDimension(KpiType.WEBSITE_PAGEVIEW.name);
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取相关数据 platform,serverTime,browserName,BrowserVersion,url
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PALTFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        String url = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL)));

        if (StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(url)) {
            logger.warn("platform 或者 serverTime 或者当前 url 不能为空 ");
            return;
        }

        long longOfTime = Long.valueOf(serverTime.trim());
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);

        //创建platform
        List<PlatFormDimension> platforms = PlatFormDimension.buildList(platform);

        //创建浏览器相关信息
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);

        //开始输出
        StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
        //设置date
        statsCommonDimension.setDate(dateDimension);
        statsCommonDimension.setKpi(this.websitePageViewDimension);
        for (PlatFormDimension pf : platforms){
            //设置platform
            statsCommonDimension.setPlatForm(pf);
            //设置kpi
            for (BrowserDimension br : browserDimensionList) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, NullWritable.get());

            }
        }
    }
}
