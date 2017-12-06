package com.wyu.transformer.mr.am;

import com.wyu.commom.GlobalConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.IOutputCollector;
import com.wyu.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author ken
 * @date 2017/11/27
 */
public class ActiveMemberCollector  implements IOutputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement psmt, IDimensionConverter converter) throws SQLException, IOException {
        // 第一步: 将key&value进行强制转换
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeMembers = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1));

        int i = 0;
        psmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        psmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        if (KpiType.BROWSER_ACTIVE_MEMBER.name.equals(statsUser.getStatsCommon().getKpi().getKpiName())) {
            // 表示输出结果是统计browser active member的，那么进行browser维度信息设置
            psmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getBrowser()));
        }
        psmt.setInt(++i, activeMembers.get());
        psmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        psmt.setInt(++i, activeMembers.get());

        // 将pstmt添加到批量执行中
        psmt.addBatch();
    }
}
