package com.wyu.transformer.mr.au;

import com.wyu.commom.GlobalConstants;
import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.IOutputCollector;
import com.wyu.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * @author ken
 */
public class HourlyActiveUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        MapWritable map = mapWritableValue.getValue();

        // 进行参数设置
        int i = 0;

        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatForm()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getKpi())); // 根据kpi

        // 设置每个小时的情况
        for (i++; i < 28; i++) {
            int v = ((IntWritable)map.get(new IntWritable(i - 4))).get();
            pstmt.setInt(i, v);
            pstmt.setInt(i + 25, v);
        }

        pstmt.setString(i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));

        // 添加到batch中
        pstmt.addBatch();
    }
}
