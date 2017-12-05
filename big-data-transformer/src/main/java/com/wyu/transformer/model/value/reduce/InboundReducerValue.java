package com.wyu.transformer.model.value.reduce;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ken
 * @date 2017/12/5
 */
public class InboundReducerValue extends BaseStatsValueWritable {

    private KpiType kpi;
    private int uvs;
    private int visit;

    public InboundReducerValue(KpiType kpi, int uvs, int visit) {
        this.kpi = kpi;
        this.uvs = uvs;
        this.visit = visit;
    }

    public InboundReducerValue() {

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

    public int getVisit() {
        return visit;
    }

    public void setVisit(int visit) {
        this.visit = visit;
    }

    @Override

    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.uvs);
        out.writeInt(this.visit);
        WritableUtils.writeEnum(out,this.kpi);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uvs = in.readInt();
        this.visit = in.readInt();
        WritableUtils.readEnum(in,KpiType.class);
    }
}
