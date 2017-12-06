package com.wyu.transformer.mr.nu;

import com.wyu.commom.GlobalConstants;
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
 * @date 2017/11/23
 */
public class StatsDeviceBrowserNewInstallUserCollector implements IOutputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement psmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        int i = 0;
        psmt.setInt(++i,converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        psmt.setInt(++i,converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        psmt.setInt(++i,converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
        psmt.setInt(++i,newInstallUsers.get());
        psmt.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        psmt.setInt(++i,newInstallUsers.get());
        psmt.addBatch();
    }
}
