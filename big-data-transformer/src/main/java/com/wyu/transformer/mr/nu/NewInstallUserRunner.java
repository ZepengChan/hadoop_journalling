package com.wyu.transformer.mr.nu;

import com.google.common.collect.Lists;
import com.wyu.commom.EventLogConstants;
import com.wyu.commom.EventLogConstants.EventEnum;
import com.wyu.commom.GlobalConstants;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.TransformerOutputFormat;
import com.wyu.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author ken
 * @date 2017/11/23
 */
public class NewInstallUserRunner implements Tool {

    private static final Logger logger = Logger.getLogger(NewInstallUserRunner.class);
    private Configuration conf = new Configuration();

    /**
     * 入口main方法
     * @param args
     */
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new NewInstallUserRunner(),args);
        } catch (Exception e) {
            logger.error("运行计算新用户的job出现异常",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        /*处理参数*/
        this.processArgs(conf, args);

        Job job = Job.getInstance(conf, "new_install_user");

        job.setJarByClass(NewInstallUserRunner.class);
//        job.setMapperClass(NewInstallUserMapper.class);
//        job.setMapOutputKeyClass();
        /*本地运行*/
//        TableMapReduceUtil.initTableMapperJob(initScan(job), NewInstallUserMapper.class, StatsUserDimension.class, TimeOutputValue.class, job, false);
        /*集群运行,本地提交和打包(jar)提交*/
        TableMapReduceUtil.initTableMapperJob(initScan(job),NewInstallUserMapper.class, StatsUserDimension.class, TimeOutputValue.class,job,true);

        job.setReducerClass(NewInstallUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        job.setOutputFormatClass(TransformerOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : -1;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.addResource("output-collector.xml");
        conf.addResource("transformer-env.xml");
        conf.addResource("query-mapping.xml");

        this.conf = HBaseConfiguration.create(conf);
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

    /**
     * 初始化scan集合
     *
     * @param job
     * @return
     */
    private List<Scan> initScan(Job job) {

        Configuration conf = job.getConfiguration();
        /*获取运行时间: yyyy-MM-dd*/
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startDate + ""));
        scan.setStopRow(Bytes.toBytes(endDate + ""));

        FilterList filterList = new FilterList();
        /*过滤数据.只分析launch事件*/
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(EventEnum.LAUNCH.alias)));
        String[] columns = new String[]{EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, EventLogConstants.LOG_COLUMN_NAME_UUID, EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, EventLogConstants.LOG_COLUMN_NAME_PALTFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION};
        filterList.addFilter(getColumnFilter(columns));
        scan.setFilter(filterList);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS));
        return Lists.newArrayList(scan);
    }

    /**
     * 获取列名过滤column
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String [] columns){
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i <length ; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }
}
