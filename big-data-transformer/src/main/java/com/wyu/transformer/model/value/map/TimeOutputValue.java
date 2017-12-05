package com.wyu.transformer.model.value.map;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.value.BaseStatsValueWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 *
 */
public class TimeOutputValue extends BaseStatsValueWritable {

    private String id;
    private long time;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }
}
