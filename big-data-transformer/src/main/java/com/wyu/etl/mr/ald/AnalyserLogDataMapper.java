package com.wyu.etl.mr.ald;

import com.wyu.commom.EventLogConstants;
import com.wyu.commom.EventLogConstants.EventEnum;
import com.wyu.etl.util.LoggerUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 自定义数据解析map类
 *
 * @author ken
 */
public class AnalyserLogDataMapper extends Mapper<Object, Text, NullWritable, Put> {

    private final Logger logger = Logger.getLogger(AnalyserLogDataMapper.class);
    private int inputRecords, filterRecords, outputRecords;
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private CRC32 crc32 = new CRC32();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        this.logger.debug("Analyse data of :" + value);
        try {
            /*解析日志*/
            Map<String, String> clientInfo = LoggerUtil.handleLog(value.toString());
            /*过滤解析失败的数据*/
            if (clientInfo.isEmpty()) {
                this.filterRecords++;
                return;
            }
            /*获取事件名称*/
            String eventAliasName = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
            EventEnum event = EventEnum.valueOfAlias(eventAliasName);
            switch (event) {
                case LAUNCH:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGEREFUND:
                case CHARGESUCCESS:
                case EVENT:
                    this.handleData(clientInfo, event, context);
                    this.outputRecords++;
                    break;
                default:
                    this.filterRecords++;
                    this.logger.warn("时间无法解析，名称为：" + eventAliasName);
            }
        } catch (Exception e) {
            this.logger.error("处理数据异常，数据 ：" + value, e);
        }

    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.info("输入数据"+this.inputRecords+",输出数据:"+this.outputRecords+",过滤数据:"+this.filterRecords);
    }

    /**
     * 真正处理数据的方法
     * @param clientInfo
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private void handleData(Map<String, String> clientInfo, EventEnum event,Context context) throws IOException, InterruptedException {
        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        String memberId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        if (StringUtils.isNotBlank(serverTime)) {
            /*要求服务器时间不为空*/
            clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);

            String rowKey = genRowKey(uuid,memberId,event.alias,serverTime);
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
                if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
                    put.add(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
            }
            context.write(NullWritable.get(),put);
        } else {
            this.filterRecords++;
        }
    }

    /**
     * 根据四个参数创建rowKey
     * @param uuid
     * @param eventAlias
     * @param memberId
     * @param serverTime
     *
     * @return
     */
    private String genRowKey(String uuid,String memberId,String eventAlias,String serverTime){

        StringBuilder sb = new StringBuilder();
        sb.append(serverTime).append("_");
        this.crc32.reset();;
        if(StringUtils.isNotBlank(uuid)){
            this.crc32.update(uuid.getBytes());
        }
        if(StringUtils.isNotBlank(memberId)){
            this.crc32.update(memberId.getBytes());
        }
        this.crc32.update(eventAlias.getBytes());
        sb.append(this.crc32.getValue() % 10000000L);
        return sb.toString();
    }
}
