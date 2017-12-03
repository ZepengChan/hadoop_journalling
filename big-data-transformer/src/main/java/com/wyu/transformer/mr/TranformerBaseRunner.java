package com.wyu.transformer.mr;

import com.google.common.collect.Lists;
import com.wyu.commom.EventLogConstants;
import com.wyu.commom.EventLogConstants.EventEnum;
import com.wyu.commom.GlobalConstants;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import java.util.List;

/**
 * 所有runner基础类
 * @author ken
 * @date 2017/12/3
 */
public class TranformerBaseRunner implements Tool {

    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private Configuration conf = null;
    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.addResource("output-collector.xml");
        conf.addResource("transformer-env.xml");
        conf.addResource("query-mapping.xml");
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * 处理参数
     *
     * @param conf
     * @param args
     */
    protected void processArgs(Configuration conf, String[] args) {
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
    protected List<Scan> initScan(Job job, List<String> columns,EventEnum eventEnum) {

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

        filterList.addFilter(getColumnFilter(columns));

        // 只需要page view事件，所以进行过滤
        filterList.addFilter(new SingleColumnValueFilter(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(eventEnum.alias)));

        scan.setFilter(filterList);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS));
        return Lists.newArrayList(scan);
    }

    /**
     * 获取列名过滤column
     *
     * @param columns
     * @return
     */
    private Filter getColumnFilter(List<String> columns) {
        int length = columns.size();
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns.get(i));
        }
        return new MultipleColumnPrefixFilter(filter);
    }
}
