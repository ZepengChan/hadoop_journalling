package com.wyu.transformer.mr.inbound;

import com.wyu.commom.DateEnum;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.StatsCommonDimension;
import com.wyu.transformer.model.dim.StatsInboundDimension;
import com.wyu.transformer.model.dim.base.DateDimension;
import com.wyu.transformer.model.dim.base.KpiDimension;
import com.wyu.transformer.model.dim.base.PlatformDimension;
import com.wyu.transformer.model.value.map.TextsOutputValue;
import com.wyu.transformer.mr.TransformerBaseMapper;
import com.wyu.transformer.service.impl.InboundDimensionService;
import com.wyu.transformer.util.UrlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** 统计location维度信息的mapper类<br/>
 * 输入: country、province、city、platform、servertime、uuid、sid<br/>
 * 一条输入对应6条输出
 * @author ken
 * @date 2017/12/5
 */
public class InboundMapper extends TransformerBaseMapper<StatsInboundDimension,TextsOutputValue> {
    private static final Logger logger = Logger.getLogger(InboundMapper.class);
    private StatsInboundDimension statsInboundDimension = new StatsInboundDimension();
    private TextsOutputValue outputValue = new TextsOutputValue();
    private KpiDimension inboundKpiDimension = new KpiDimension(KpiType.INBOUND.name);
    private Map<String, Integer> inbounds = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            // 获取inbound相关数据
            this.inbounds = InboundDimensionService.getInboundByType(context.getConfiguration(), 0);
        } catch (SQLException e) {
            logger.error("获取外链id出现数据库异常", e);
            throw new IOException("出现异常", e);
        }
    }
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        // 获取数据
        String platform = this.getPlatform(value);
        String serverTime = this.getServerTime(value);
        String referrerUrl = this.getReferrerUrl(value);
        String uuid = this.getUuid(value);
        String sid = this.getSessionId(value);

        // 过滤无效数据
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(uuid) || StringUtils.isBlank(sid) || StringUtils.isBlank(referrerUrl) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("平台&uuid&会话id&前一个页面的url&服务器时间不能为空，而且服务器时间必须为时间戳形式。");
            this.filterRecords++;
            return;
        }
        int inboundId;
        try {
            inboundId = this.getInboundIdByHost(UrlUtil.getHost(referrerUrl));
        } catch (Throwable e) {
            logger.warn("获取referrer url对应的inbound id异常", e);
            inboundId = 0;
        }
        // 过滤无效inbound id
        if (inboundId <= 0) {
            // 如果获取的inbound id小于等于0，那么表示无效inbound
            logger.warn("该url对应的不是外链url:" + referrerUrl);
            this.filterRecords++;
            return;
        }
        // platform维度创建
        List<PlatformDimension> platforms = PlatformDimension.buildList(platform);

        // 构建输出对象
        this.outputValue.setSid(sid);
        this.outputValue.setUuid(uuid);
        StatsCommonDimension statsCommon = this.statsInboundDimension.getStatsCommon();
        statsCommon.setDate(DateDimension.buildDate(Long.valueOf(serverTime.trim()), DateEnum.DAY));
        statsCommon.setKpi(this.inboundKpiDimension);

        // 输出
        for (PlatformDimension pf : platforms) {
            statsCommon.setPlatform(pf);

            // 输出全部inbound维度
            this.statsInboundDimension.getInbound().setId(InboundDimensionService.ALL_OF_INBOUND_ID);
            context.write(this.statsInboundDimension, this.outputValue);
            this.outputRecords++;

            // 输出具体inbound的维度
            this.statsInboundDimension.getInbound().setId(inboundId);
            context.write(this.statsInboundDimension, this.outputValue);
            this.outputRecords++;
        }
    }

    /**
     * 根据url的host来获取不同的inbound
     * id值，如果该host是统计统计网站的本身host，那么直接返回0，也就是说如果host不属于外链，那么返回0
     *
     * @param host
     * @return
     */
    private int getInboundIdByHost(String host) {
        int id = 0;
        if (UrlUtil.isValidateInboundHost(host)) {
            // 是一个有效的外链host，那么进行inbound id获取操作
            id = InboundDimensionService.OTHER_OF_INBOUND_ID;

            // 查看是否是一个具体的inbound id值
            for (Map.Entry<String, Integer> entry : this.inbounds.entrySet()) {
                String urlRegex = entry.getKey();
                if (host.equals(urlRegex) || host.startsWith(urlRegex) || host.matches(urlRegex)) {
                    id = entry.getValue();
                    break;
                }
            }
        }
        return id;
    }
}
