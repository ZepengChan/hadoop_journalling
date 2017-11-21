package com.wyu.transformer.mr.nu;

import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author ken
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

    }
}
