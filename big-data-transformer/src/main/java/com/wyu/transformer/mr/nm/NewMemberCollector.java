package com.wyu.transformer.mr.nm;

import com.wyu.commom.GlobalConstants;
import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.IOutputCollector;
import com.wyu.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author ken
 * @date 2017/11/27
 */
public class NewMemberCollector implements IOutputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement psmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUser = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newMembers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        int i = 0;
        switch (mapWritableValue.getKpi()){
            case NEW_MEMBER:
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatForm()));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
                psmt.setInt(++i,newMembers.get());
                psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                psmt.setInt(++i,newMembers.get());
                break;
            case BROWSER_NEW_MEMBER:
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatForm()));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getBrowser()));
                psmt.setInt(++i,newMembers.get());
                psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                psmt.setInt(++i,newMembers.get());
                break;
                default:
                    throw new RuntimeException("不支持的kpi输出操作! kpi:" + mapWritableValue.getKpi().name);
        }
        psmt.addBatch();
    }
}
