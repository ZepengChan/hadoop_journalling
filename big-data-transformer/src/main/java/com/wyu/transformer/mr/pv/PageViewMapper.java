package com.wyu.transformer.mr.pv;

import com.wyu.commom.DateEnum;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.StatsCommonDimension;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.dim.base.*;
import com.wyu.transformer.mr.TransformerBaseMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
public class PageViewMapper extends TransformerBaseMapper<StatsUserDimension,NullWritable> {
    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private  KpiDimension websitePageViewDimension = new KpiDimension(KpiType.WEBSITE_PAGEVIEW.name);
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
    	 // 1. 获取platform、time、url
        String platform = super.getPlatform(value);
        String serverTime = super.getServerTime(value);
        String url = super.getCurrentUrl(value);

        // 2. 过滤数据
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(url) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("平台&服务器时间&当前url不能为空，而且服务器时间必须为时间戳形式的字符串");
            this.filterRecords++;
            return ;
        }

        // 3. 创建platform维度信息
        List<PlatformDimension> platforms = PlatformDimension.buildList(platform);
        // 4. 创建browser维度信息
        String browserName = super.getBrowserName(value);
        String browserVersion = super.getBrowserVersion(value);
        List<BrowserDimension> browsers = BrowserDimension.buildList(browserName, browserVersion);
        // 5. 创建date维度信息
        DateDimension dayOfDimenion = DateDimension.buildDate(Long.valueOf(serverTime.trim()), DateEnum.DAY);

        // 6. 输出的写出
        StatsCommonDimension statsCommon = this.statsUserDimension.getStatsCommon();
        statsCommon.setDate(dayOfDimenion); // 设置date dimension
        statsCommon.setKpi(this.websitePageViewDimension); // 设置kpi dimension
        for (PlatformDimension pf : platforms) {
            statsCommon.setPlatform(pf); // 设置platform dimension
            for (BrowserDimension br : browsers) {
                this.statsUserDimension.setBrowser(br); // 设置browser dimension
                // 输出
                context.write(this.statsUserDimension, NullWritable.get());
                this.outputRecords++;
            }
        }
    }
}
