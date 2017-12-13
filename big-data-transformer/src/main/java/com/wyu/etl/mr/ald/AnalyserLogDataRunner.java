package com.wyu.etl.mr.ald;

import com.wyu.commom.EventLogConstants;
import com.wyu.commom.GlobalConstants;
import com.wyu.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * mapreduce runner类
 *
 * @author ken
 */
public class AnalyserLogDataRunner implements Tool {

    private static final Logger logger = Logger.getLogger(AnalyserLogDataRunner.class);
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new AnalyserLogDataRunner(),args);
        } catch (Exception e) {
            logger.error("执行日志解析异常",e);
            throw new RuntimeException(e);
        }
    }
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        this.processArgs(conf, args);

        Job job = Job.getInstance(conf, "analyser_logdata");
        job.setJarByClass(AnalyserLogDataRunner.class);
        job.setMapperClass(AnalyserLogDataMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        /*本地*/
//        TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null, job, null, null, null, null, false);
        /*集群*/
        TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null, job, null, null, null, null);
        job.setNumReduceTasks(0);

        /*设置输入路径*/
        this.setJobInputPaths(job);
        return job.waitForCompletion(true) ? 0 : -1;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create(conf);
    }

    /**
     * 设置输入路径
     *
     * @param job
     */
    private void setJobInputPaths(Job job) {
        Configuration conf = job.getConfiguration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
            Path inputPath = new Path("/logs/12/12" /*+ TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "MM/dd")*/);
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            } else {
                throw new RuntimeException("输入文件不存在！文件路径：" + inputPath);
            }

        } catch (Exception e) {
            throw new RuntimeException("设置Job的MapReduce输入路径异常！", e);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 处理参数
     *
     * @param conf
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[++i];
                    break;
                }
            }
        }

        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            date = TimeUtil.getYesterday();
        }

        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }
}
