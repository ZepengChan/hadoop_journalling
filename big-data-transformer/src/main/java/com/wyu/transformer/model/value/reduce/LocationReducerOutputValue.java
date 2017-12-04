package com.wyu.transformer.model.value.reduce;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义location reducer 输出value类
 * @author ken
 * @date 2017/12/4
 */
public class LocationReducerOutputValue extends BaseStatsValueWritable{

    private KpiType kpi;
    //活跃用户数
    private int uvs;
    //会话个数
    private int visits;
    //跳出会话个数
    private int bounceNumber;

    public LocationReducerOutputValue() {
    }

    public LocationReducerOutputValue(KpiType kpi, int uvs, int visits, int bounceNumber) {
        this.kpi = kpi;
        this.uvs = uvs;
        this.visits = visits;
        this.bounceNumber = bounceNumber;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getUvs() {
        return uvs;
    }

    public void setUvs(int uvs) {
        this.uvs = uvs;
    }

    public int getVisits() {
        return visits;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }

    public int getBounceNumber() {
        return bounceNumber;
    }

    public void setBounceNumber(int bounceNumber) {
        this.bounceNumber = bounceNumber;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.uvs);
        out.writeInt(this.visits);
        out.writeInt(this.bounceNumber);
        WritableUtils.writeEnum(out,this.kpi);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uvs = in.readInt();
        this.visits = in.readInt();
        this.bounceNumber = in.readInt();
        this.kpi = WritableUtils.readEnum(in,KpiType.class);
    }
}
