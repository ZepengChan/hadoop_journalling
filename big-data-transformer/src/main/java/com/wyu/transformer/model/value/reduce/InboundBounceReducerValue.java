package com.wyu.transformer.model.value.reduce;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class InboundBounceReducerValue extends BaseStatsValueWritable {
    private KpiType kpi;
    private int bounceNumber;

    public InboundBounceReducerValue() {
        super();
    }

    public InboundBounceReducerValue(int bounceNumber) {
        super();
        this.bounceNumber = bounceNumber;
    }

    public int getBounceNumber() {
        return bounceNumber;
    }

    public void setBounceNumber(int bounceNumber) {
        this.bounceNumber = bounceNumber;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    /**
     * 自增1
     */
    public void incrBounceNum() {
        this.bounceNumber = this.bounceNumber + 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.bounceNumber);
        WritableUtils.writeEnum(out, this.kpi);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.bounceNumber = in.readInt();
        this.kpi = WritableUtils.readEnum(in, KpiType.class);
    }

    @Override
    public KpiType getKpi() {
        return kpi;
    }

}
