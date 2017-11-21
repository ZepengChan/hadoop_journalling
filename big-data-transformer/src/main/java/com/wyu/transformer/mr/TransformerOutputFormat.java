package com.wyu.transformer.mr;

import com.wyu.commom.GlobalConstants;
import com.wyu.commom.KpiType;
import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import com.wyu.transformer.service.IDimensionConverter;
import com.wyu.transformer.service.impl.DimensionConverterImpl;
import com.wyu.util.JdbcManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TransformerOutputFormat extends OutputFormat<BaseDimension, BaseStatsValueWritable> {

    private static final Logger logger = Logger.getLogger(TransformerOutputFormat.class);

    @Override
    public RecordWriter<BaseDimension, BaseStatsValueWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Connection conn = null;
        IDimensionConverter converter = new DimensionConverterImpl();
        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            logger.error("获取数据库连接失败", e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return new TransformerRecordWriter(conn, conf, converter);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        /*检测输出空间，输出到mysql不用检测*/
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    /**
     * 自定义具体数据输出writer
     *
     * @author ken
     */
    public class TransformerRecordWriter extends RecordWriter<BaseDimension, BaseStatsValueWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConverter converter = null;
        private Map<KpiType, PreparedStatement> map = new HashMap<>();
        private Map<KpiType, Integer> batch = new HashMap<>();

        public TransformerRecordWriter(Connection conn, Configuration conf, IDimensionConverter converter) {
            this.conn = conn;
            this.conf = conf;
            this.converter = converter;
        }

        @Override
        public void write(BaseDimension key, BaseStatsValueWritable value) throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }

            try {
                KpiType kpi = value.getKpi();
                PreparedStatement psmt = null;
                int count = 1;
                if (map.get(key) == null) {
                    psmt = this.conn.prepareStatement(conf.get(kpi.name));
                    map.put(kpi, psmt);
                    batch.put(kpi, 1);
                } else {
                    psmt = map.get(kpi);
                    count = batch.get(kpi);
                    count++;
                }
                batch.put(kpi, count);

                String collectorName = conf.get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpi.name);
                Class<?> clazz = Class.forName(collectorName);
                IOutputCollector collector = (IOutputCollector) clazz.newInstance();
                collector.collect(conf, key, value, psmt, converter);
                if (count % Integer.valueOf(conf.get(GlobalConstants.JDBC_BATCH_NUMBER, GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER)) == 0) {
                    psmt.addBatch();
                    conn.commit();
                    batch.remove(kpi);
                }


            } catch (Exception e) {
                logger.error("写数据出现异常", e);
                throw new IOException(e);
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for (Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                    entry.getValue().executeUpdate();
                }
            } catch (Exception e) {
                logger.error("执行executeUpdate方法异常", e);
                throw new IOException(e);
            } finally {
                if (conn != null) {
                    try {
                        conn.commit();
                    } catch (SQLException e) {
                        /*nothing*/
                    } finally {
                        for (Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                            try {
                                entry.getValue().close();
                            } catch (SQLException e) {
                                /*nothing*/
                            }
                        }
                    }
                }
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        /*nothing*/
                    }
                }

            }
        }

    }
}
