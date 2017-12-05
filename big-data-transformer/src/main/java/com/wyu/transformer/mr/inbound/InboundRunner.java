package com.wyu.transformer.mr.inbound;

import com.wyu.commom.EventLogConstants;
import com.wyu.commom.EventLogConstants.EventEnum;
import com.wyu.transformer.model.dim.StatsInboundDimension;
import com.wyu.transformer.model.value.map.TextsOutputValue;
import com.wyu.transformer.model.value.reduce.InboundReducerValue;
import com.wyu.transformer.mr.TransformerBaseRunner;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * @author ken
 * @date 2017/12/5
 */
public class InboundRunner extends TransformerBaseRunner {

    private static final Logger logger = Logger.getLogger(InboundRunner.class);

    public static void main(String[] args) {
        InboundRunner runner = new InboundRunner();
        runner.setupRunner("inbound", InboundRunner.class, InboundMapper.class, InboundReducer.class, StatsInboundDimension.class, TextsOutputValue.class, StatsInboundDimension.class, InboundReducerValue.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("执行异常", e);
            throw new RuntimeException("执行异常", e);
        }
    }

    @Override
    protected Filter fetchHbaseFilter() {
        FilterList list = new FilterList();
        String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_REFERRER_URL, // 前一个页面的url
                EventLogConstants.LOG_COLUMN_NAME_UUID, // uuid
                EventLogConstants.LOG_COLUMN_NAME_SESSION_ID, // 会话id
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME // 事件名称
        };
        list.addFilter(this.getColumnFilter(columns));
        list.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(EventEnum.PAGEVIEW.alias)));

        return list;
    }
}
