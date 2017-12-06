package com.wyu.transformer.mr.pv;

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
 * @date 2017/11/30
 */
public class PageViewCollector implements IOutputCollector{

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUser = (StatsUserDimension) key;
        int pv = ((IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1))).get();

        // 参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getBrowser()));
        pstmt.setInt(++i, pv);
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, pv);

        // 加入batch
        pstmt.addBatch();
    }
}
