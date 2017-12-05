package com.wyu.transformer.mr.inbound.bounce;

import com.wyu.transformer.model.dim.StatsCommonDimension;
import com.wyu.transformer.model.dim.StatsInboundBounceDimension;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义的二次排序使用到的类
 * 
 * @author ken
 *
 */
public class InboundBounceSecondSort {
    /**
     * 自定义分组类
     * 
     * @author ken
     *
     */
    public static class InboundBounceGroupingComparator extends WritableComparator {
        public InboundBounceGroupingComparator() {
            super(StatsInboundBounceDimension.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StatsInboundBounceDimension key1 = (StatsInboundBounceDimension) a;
            StatsInboundBounceDimension key2 = (StatsInboundBounceDimension) b;
            return key1.getStatsCommon().compareTo(key2.getStatsCommon());
        }
    }

    /**
     * 自定义reducer分组函数
     * 
     * @author ken
     *
     */
    public static class InboundBouncePartitioner extends Partitioner<StatsInboundBounceDimension, IntWritable> {
        private HashPartitioner<StatsCommonDimension, IntWritable> partitioner = new HashPartitioner<>();

        @Override
        public int getPartition(StatsInboundBounceDimension key, IntWritable value, int numPartitions) {
            return this.partitioner.getPartition(key.getStatsCommon(), value, numPartitions);
        }
    }
}
