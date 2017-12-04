package com.wyu.transformer.mr.location;

import com.wyu.commom.EventLogConstants;
import com.wyu.commom.EventLogConstants.EventEnum;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.value.map.TextsOutputValue;
import com.wyu.transformer.model.value.reduce.LocationReducerOutputValue;
import com.wyu.transformer.mr.TransformerBaseRunner;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * @author ken
 * @date 2017/12/4
 */
public class LocationRunner extends TransformerBaseRunner {
    private static final Logger logger = Logger.getLogger(LocationRunner.class);

    public static void main(String[] args) {
        LocationRunner runner = new LocationRunner();
        runner.setupRunner("Location", LocationRunner.class, LocationMapper.class, LocationReducer.class, StatsUserDimension.class, TextsOutputValue.class, StatsUserDimension.class, LocationReducerOutputValue.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行LocationRunner任务出现异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Filter fetchHbaseFilter() {
        FilterList filterList = new FilterList();
        // 定义mapper中需要获取的列名
        String[] columns = new String[] {
                EventLogConstants.LOG_COLUMN_NAME_COUNTRY, // 国家
                EventLogConstants.LOG_COLUMN_NAME_PROVINCE, // 省份
                EventLogConstants.LOG_COLUMN_NAME_CITY, // 城市
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_UUID, // uuid
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_SESSION_ID, // 会话id
        };
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME),Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(EventEnum.PAGEVIEW.alias)));
        filterList.addFilter(this.getColumnFilter(columns));
        return filterList;
    }
}
