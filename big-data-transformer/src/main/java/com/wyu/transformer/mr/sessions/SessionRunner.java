package com.wyu.transformer.mr.sessions;

import com.wyu.commom.EventLogConstants;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.TransformerBaseRunner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.log4j.Logger;

/**
 * 统计会话个数&会话长度runner类
 * @author ken
 * @date 2017/11/28
 */
public class SessionRunner extends TransformerBaseRunner {

    private static final Logger logger = Logger.getLogger(SessionRunner.class);

    public static void main(String[] args) {
        SessionRunner runner = new SessionRunner();
        runner.setupRunner("SessionRunner",SessionRunner.class,SessionMapper.class,SessionReducer.class,StatsUserDimension.class, TimeOutputValue.class, StatsUserDimension.class, MapWritableValue.class);

        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行SessionRunner任务出现异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Filter fetchHbaseFilter() {
        FilterList filterList = new FilterList();
        // 定义mapper中需要获取的列名
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_SESSION_ID,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION
        };
        filterList.addFilter(this.getColumnFilter(columns));
        return filterList;
    }

}
