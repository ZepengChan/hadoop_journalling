package com.wyu.transformer.mr.nm;

import com.wyu.commom.DateEnum;
import com.wyu.commom.GlobalConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.*;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.mr.TranformerBaseMapper;
import com.wyu.transformer.mr.am.ActiveMemberMapper;
import com.wyu.transformer.util.MemberUtil;
import com.wyu.util.JdbcManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * 计算new member mapper类
 *
 * @author ken
 * @date 2017/11/27
 */
public class NewMemberMapper extends TranformerBaseMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension newMemberKpi = new KpiDimension(KpiType.NEW_MEMBER.name);
    private KpiDimension newMemberBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.name);
    private Connection conn = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //进行数据库conn初始化操作
        Configuration conf = context.getConfiguration();
        try {
            this.conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            MemberUtil.deleteMemberInfoByDate(conf.get(GlobalConstants.RUNNING_DATE_PARAMES), conn);
        } catch (SQLException e) {
            logger.error("获取数据库连接异常",e);
            throw new IOException(e);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        String memberId = super.getMemberId(value);
        String platform = super.getPlatform(value);
        String serverTime = super.getServerTime(value);
        /**
         * 判断memberid是否是第一次访问
         */
        try {
            if (StringUtils.isBlank(memberId) || !MemberUtil.isNewMemberId(memberId, conn) || !MemberUtil.isValidateMemberId(memberId)) {
                logger.warn("member id 不能为空 且必须是第一次访问的member id");
                this.filterRecords++;
                return;
            }
        } catch (SQLException e) {
            logger.warn("查询member id 是否是新id出现异常", e);
            throw new RuntimeException("查询数据库出现异常", e);
        }
        //过滤无效数据
        if (StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            System.out.println("memberId:" + memberId
                    + "\tplatform:" + platform
                    + "\tserverTime:" + serverTime);
            logger.warn("uuid & serverTime & platform不能为空!");
            this.filterRecords++;
            return;
        }


        //获取日期相关信息,其中id是memberid
        long longOfTime = Long.valueOf(serverTime.trim());
        this.outputValue.setTime(longOfTime);
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
        this.outputValue.setId(memberId);

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
            //设置kpi
            statsCommonDimension.setKpi(newMemberKpi);
            context.write(this.outputKey, this.outputValue);
            this.outputRecords++;

            //browser 维度统计
            statsCommonDimension.setKpi(newMemberBrowserKpi);
            for (BrowserDimension br : browserDimensionList) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
                this.outputRecords++;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if(this.conn != null){
            try {
                this.conn.close();
            } catch (SQLException e) {
                //nothing
            }
        }
    }
}
