package com.wyu.transformer.mr;

import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import com.wyu.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 自定义的配合自定义output进行具体sql的输出类
 * @author ken
 */
public interface IOutputCollector {

    /**
     * 具体执行统计数据插入的方法
     * @param conf
     * @param key
     * @param value
     * @param psmt
     * @param converter
     * @throws SQLException
     */
    void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement psmt, IDimensionConverter converter) throws SQLException;
}
