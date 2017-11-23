package com.wyu.transformer.model.value.reduce;

import com.wyu.commom.KpiType;
import com.wyu.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ken
 * @date 2017/11/23
 */
public class MapWritableValue extends BaseStatsValueWritable {

    private MapWritable value = new MapWritable();
    private KpiType kpi;

    public MapWritableValue(MapWritable value, KpiType kpi) {
        this.value = value;
        this.kpi = kpi;
    }

    public MapWritableValue() {
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out);
        WritableUtils.writeEnum(out,this.kpi);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        this.kpi = WritableUtils.readEnum(in,KpiType.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapWritableValue that = (MapWritableValue) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;
        return kpi == that.kpi;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (kpi != null ? kpi.hashCode() : 0);
        return result;
    }
}
