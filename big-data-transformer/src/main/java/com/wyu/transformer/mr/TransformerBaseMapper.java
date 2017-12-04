package com.wyu.transformer.mr;

import com.wyu.commom.EventLogConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 公共mapper类,主要提供技术和hbase value的获取
 * @author ken
 * @date 2017/12/3
 */
public class TransformerBaseMapper<KEYOUT,VALUEOUT> extends TableMapper<KEYOUT,VALUEOUT>{

    private static final Logger logger = Logger.getLogger(TransformerBaseMapper.class);
    protected Configuration conf = null;
    private long startTime = System.currentTimeMillis();
    //输入数
    protected int inputRecords = 0;
    //过滤数
    protected int filterRecords = 0;
    //输出数
    protected int outputRecords = 0;

    private static byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);

    /**
     * 初始化conf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.conf = context.getConfiguration();
    }


    /**
     * 最后打印提示信息
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try {
            //打印提示信息:运行时间,输入记录数,过滤记录数,输出记录数
            StringBuilder sb = new StringBuilder();
            long endTime = System.currentTimeMillis();
            sb.append("job_id:").append(context.getJobID().toString());
            sb.append("; start_time:").append(this.startTime);
            sb.append("; end_time:").append(endTime);
            sb.append("; using_time:").append(endTime - this.startTime).append("ms");
            sb.append("; inout records:").append(this.inputRecords);
            sb.append("; filter records:").append(this.filterRecords);
            sb.append("; output records:").append(this.outputRecords);
            System.out.println(sb.toString());

            logger.info(sb.toString());
        }catch (Throwable e){
            //nothing
        }
    }

    public String fetchValue(Result value,String name){
        return Bytes.toString(value.getValue(family, Bytes.toBytes(name)));
    }
    public String getUuid(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_UUID);
    }

    public String getMemberId(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
    }

    public String getServerTime(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
    }

    public String getPlatform(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_PALTFORM);
    }

    public String getBrowserName(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME);
    }

    public String getBrowserVersion(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION);
    }

    public String getSessionId(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_SESSION_ID);
    }

    public String getUrl(Result value){
        return this.fetchValue(value,EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL);
    }


}
