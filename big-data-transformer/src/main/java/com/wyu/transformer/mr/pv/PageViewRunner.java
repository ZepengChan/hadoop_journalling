package com.wyu.transformer.mr.pv;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.wyu.commom.EventLogConstants;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.TransformerBaseRunner;

/** 统计PV 入口类
 * @author ken
 * @date 2017/11/30
 */
public class PageViewRunner extends TransformerBaseRunner {

    private static final Logger logger = Logger.getLogger(PageViewRunner.class);

    public static void main(String[] args) {
        PageViewRunner runner = new PageViewRunner();
        runner.setupRunner("PageViewRunner",PageViewRunner.class,PageViewMapper.class,PageViewReducer.class,StatsUserDimension.class, NullWritable.class, StatsUserDimension.class, MapWritableValue.class);

        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行PageViewRunner任务出现异常", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    protected Filter fetchHbaseFilter() {
        FilterList filterList = new FilterList();
        // 定义mapper中需要获取的列名
        String[] columns = new String[] {
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, // 获取事件名称
                EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL, // 当前url
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION // 浏览器版本号
        };
        /*过滤数据.只分析launch事件*/
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias)));
        filterList.addFilter(this.getColumnFilter(columns));
        return filterList;
    }

}
