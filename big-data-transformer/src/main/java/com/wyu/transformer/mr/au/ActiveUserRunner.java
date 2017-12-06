package com.wyu.transformer.mr.au;

import com.wyu.commom.EventLogConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.TransformerBaseRunner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author ken
 * @date 2017/11/26
 */
public class ActiveUserRunner extends TransformerBaseRunner {

    private static final Logger logger = Logger.getLogger(ActiveUserRunner.class);

    public static void main(String[] args) {
        ActiveUserRunner runner = new ActiveUserRunner();
        runner.setupRunner("active-user", ActiveUserRunner.class, ActiveUserMapper.class, ActiveUserReducer.class, StatsUserDimension.class, TimeOutputValue.class, StatsUserDimension.class, MapWritableValue.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行active user任务出现异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void beforeRunJob(Job job) throws IOException {
        super.beforeRunJob(job);
        //设置reduce个数,并分区
        job.setNumReduceTasks(3);
        job.setPartitionerClass(ActiveUserPartitioner.class);
        //布帝洞推测执行
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
    }

    @Override
    protected void configure(String... resourceFiles) {
        super.configure(resourceFiles);
        conf.set("mapred,child.java.opts","-Xmx500m");
        conf.set("mapreduce.map.output.compress","true");
    }

    @Override
    protected Filter fetchHbaseFilter() {
        FilterList filterList = new FilterList();
        // 定义mapper中需要获取的列名
        String[] columns = new String[] {
                EventLogConstants.LOG_COLUMN_NAME_UUID, // 用户id
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION // 浏览器版本号
        };
        filterList.addFilter(this.getColumnFilter(columns));
        return filterList;
    }

    /**
     * 自定义分区类
     */
    public static class ActiveUserPartitioner extends Partitioner<StatsUserDimension, TimeOutputValue> {

        @Override
        public int getPartition(StatsUserDimension key, TimeOutputValue value, int i) {
            String kpi = key.getStatsCommon().getKpi().getKpiName();
            if(KpiType.ACTIVE_USER.name.equals(kpi)){
                return 0;
            }else if(KpiType.BROWSER_ACTIVE_USER.name.equals(kpi)){
                return 1;
            }else if(KpiType.HOURLY_ACTIVE_USER.name.equals(kpi)){
                return 2;
            }

            throw  new IllegalArgumentException("无法获取分区ID,当前kpi为:" +kpi+"reduce 个数为" + i);
        }
    }
}


