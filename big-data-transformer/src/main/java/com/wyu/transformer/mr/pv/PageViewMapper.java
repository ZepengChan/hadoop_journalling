package com.wyu.transformer.mr.pv;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.wyu.commom.DateEnum;
import com.wyu.commom.EventLogConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.BrowserDimension;
import com.wyu.transformer.model.dim.base.DateDimension;
import com.wyu.transformer.model.dim.base.KpiDimension;
import com.wyu.transformer.model.dim.base.PlatFormDimension;
import com.wyu.transformer.model.dim.base.StatsCommonDimension;
import com.wyu.transformer.model.dim.base.StatsUserDimension;

/**统计pv mapper类
 * 输入hbase数据,包括platform,serverTime,browserName,BrowserVersion,url
 * 输出<StatsUserDimension,NullWritable>键值对
 * @author ken
 * @date 2017/11/30
 */
public class PageViewMapper extends TableMapper<StatsUserDimension,NullWritable> {
    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private  KpiDimension websitePageViewDimension = new KpiDimension(KpiType.WEBSITE_PAGEVIEW.name);
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
    	 // 1. 获取platform、time、url
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PALTFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String url = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL)));

        // 2. 过滤数据
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(url) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("平台&服务器时间&当前url不能为空，而且服务器时间必须为时间戳形式的字符串");
            return ;
        }

        // 3. 创建platform维度信息
        List<PlatFormDimension> platforms = PlatFormDimension.buildList(platform);
        // 4. 创建browser维度信息
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browsers = BrowserDimension.buildList(browserName, browserVersion);
        // 5. 创建date维度信息
        DateDimension dayOfDimenion = DateDimension.buildDate(Long.valueOf(serverTime.trim()), DateEnum.DAY);

        // 6. 输出的写出
        StatsCommonDimension statsCommon = this.statsUserDimension.getStatsCommon();
        statsCommon.setDate(dayOfDimenion); // 设置date dimension
        statsCommon.setKpi(this.websitePageViewDimension); // 设置kpi dimension
        for (PlatFormDimension pf : platforms) {
            statsCommon.setPlatForm(pf); // 设置platform dimension
            for (BrowserDimension br : browsers) {
                this.statsUserDimension.setBrowser(br); // 设置browser dimension
                // 输出
                context.write(this.statsUserDimension, NullWritable.get());
            }
        }
    }
}
