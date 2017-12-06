package com.wyu.transformer.mr.inbound.bounce;

import com.wyu.commom.GlobalConstants;
import com.wyu.transformer.model.dim.StatsInboundDimension;
import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import com.wyu.transformer.model.value.reduce.InboundBounceReducerValue;
import com.wyu.transformer.mr.IOutputCollector;
import com.wyu.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class InboundBounceCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsInboundDimension inboundDimension = (StatsInboundDimension) key;
        InboundBounceReducerValue inboundReduceValue = (InboundBounceReducerValue) value;

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, inboundDimension.getInbound().getId()); // 直接设置，在mapper类中已经设置
        pstmt.setInt(++i, inboundReduceValue.getBounceNumber());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, inboundReduceValue.getBounceNumber());

        pstmt.addBatch();
    }

}
