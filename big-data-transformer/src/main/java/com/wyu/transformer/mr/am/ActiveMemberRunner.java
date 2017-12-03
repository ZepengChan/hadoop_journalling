package com.wyu.transformer.mr.am;

import com.wyu.commom.EventLogConstants;
import com.wyu.commom.EventLogConstants.EventEnum;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.TranformerBaseRunner;
import com.wyu.transformer.mr.TransformerOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ken
 * @date 2017/11/26
 */
public class ActiveMemberRunner extends TranformerBaseRunner {

    private static final Logger logger = Logger.getLogger(ActiveMemberRunner.class);

    public static void main(String[] args) {
        try {
            ToolRunner.run(new ActiveMemberRunner(),args);
        } catch (Exception e) {
            logger.error("运行active_user出现异常!"+ e);
            throw  new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        /*处理参数*/
        super.processArgs(conf, args);

        Job job = Job.getInstance(conf, "active_member");

        job.setJarByClass(ActiveMemberRunner.class);
        /*本地运行*/
        TableMapReduceUtil.initTableMapperJob(initScan(job), ActiveMemberMapper.class, StatsUserDimension.class, TimeOutputValue.class, job, false);
        /*集群运行*/
//        TableMapReduceUtil.initTableMapperJob(initScan(job),ActiveMemberMapper.class, StatsUserDimension.class, TimeOutputValue.class,job);

        job.setReducerClass(ActiveMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        job.setOutputFormatClass(TransformerOutputFormat.class);
        // 开始毫秒数
        long startTime = System.currentTimeMillis();
        try {
            return job.waitForCompletion(true) ? 0 : -1;
        } finally {
            // 结束的毫秒数
            long endTime = System.currentTimeMillis();
            logger.info("Job<" + job.getJobName() + ">是否执行成功:" + job.isSuccessful() + "; 开始时间:" + startTime + "; 结束时间:" + endTime + "; 用时:" + (endTime - startTime) + "ms");
        }
    }



    /**
     * 初始化scan集合
     *
     * @param job
     * @return
     */
    private List<Scan> initScan(Job job) {

        List<String> columns = new ArrayList<>();
        columns.add(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        columns.add(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        columns.add(EventLogConstants.LOG_COLUMN_NAME_PALTFORM);
        columns.add(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME);
        columns.add(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION);
        columns.add(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);

        return super.initScan(job,columns,EventEnum.PAGEVIEW);
    }
}
