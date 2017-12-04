package com.wyu.transformer.model.value.map;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一系列字符串输出类
 *
 * @author ken
 * @date 2017/12/4
 */
public class TextsOutputValue extends BaseStatsValueWritable {

    //用户唯一标识符
    private String uuid;
    //会话id
    private String sid;

    private KpiType kpi;



    public TextsOutputValue(String uuid, String sid) {
        this.uuid = uuid;
        this.sid = sid;
    }

    public TextsOutputValue() {
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }
    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        this.internalWrite(out,this.uuid);
        this.internalWrite(out,this.sid);

    }
    private void internalWrite(DataOutput out,String value) throws IOException {
        if(StringUtils.isEmpty(value)){
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = this.internalReadString(in);
        this.sid = this.internalReadString(in);
    }

    private String internalReadString(DataInput in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
