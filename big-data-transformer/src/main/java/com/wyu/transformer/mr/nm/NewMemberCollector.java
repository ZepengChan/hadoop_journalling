package com.wyu.transformer.mr.nm;

import com.wyu.commom.GlobalConstants;
import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.IOutputCollector;
import com.wyu.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

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

        int i = 0;
        switch (mapWritableValue.getKpi()){
            case NEW_MEMBER:
                IntWritable v1 = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
                psmt.setInt(++i,v1.get());
                psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                psmt.setInt(++i,v1.get());
                break;
            case BROWSER_NEW_MEMBER:
                IntWritable v2 = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
                psmt.setInt(++i,converter.getDimensionIdByValue(statsUser.getBrowser()));
                psmt.setInt(++i,v2.get());
                psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                psmt.setInt(++i,v2.get());
                break;
            case INSERT_MEMBER_INFO:
                Text v3 = (Text) mapWritableValue.getValue().get(new IntWritable(-1));
                psmt.setString(++i,v3.toString());
                psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                break;
                default:
                    throw new RuntimeException("不支持的kpi输出操作! kpi:" + mapWritableValue.getKpi().name);
        }
        psmt.addBatch();
    }
}
