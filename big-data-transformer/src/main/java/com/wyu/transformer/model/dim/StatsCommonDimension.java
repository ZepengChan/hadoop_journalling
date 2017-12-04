package com.wyu.transformer.model.dim;

import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.dim.base.DateDimension;
import com.wyu.transformer.model.dim.base.KpiDimension;
import com.wyu.transformer.model.dim.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 公用的dimension的信息集合
 *
 * @author ken
 */
public class StatsCommonDimension extends StatsDimension {

    private DateDimension date = new DateDimension();
    private PlatformDimension platForm = new PlatformDimension();
    private KpiDimension kpi = new KpiDimension();

    public StatsCommonDimension(DateDimension date, PlatformDimension platForm, KpiDimension kpi) {
        this.date = date;
        this.platForm = platForm;
        this.kpi = kpi;
    }

    public StatsCommonDimension() {
    }

    public DateDimension getDate() {
        return date;
    }

    public void setDate(DateDimension date) {
        this.date = date;
    }

    public PlatformDimension getPlatform() {
        return platForm;
    }

    public void setPlatform(PlatformDimension platForm) {
        this.platForm = platForm;
    }

    public KpiDimension getKpi() {
        return kpi;
    }

    public void setKpi(KpiDimension kpi) {
        this.kpi = kpi;
    }

    /**
     * 克隆一个实例对象
     *
     * @param dimension
     * @return
     */
    public static StatsCommonDimension clone(StatsCommonDimension dimension) {
        DateDimension date = new DateDimension(dimension.date.getId(), dimension.date.getYear(), dimension.date.getSeason(), dimension.date.getMonth(), dimension.date.getWeek(), dimension.date.getDay(), dimension.date.getType(), dimension.date.getCalendar());
        PlatformDimension platForm = new PlatformDimension(dimension.platForm.getId(), dimension.platForm.getPlatformName());
        KpiDimension kpi = new KpiDimension(dimension.kpi.getId(), dimension.kpi.getKpiName());
        return new StatsCommonDimension(date, platForm, kpi);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o) {
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.date.compareTo(other.date);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.platForm.compareTo(other.platForm);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.kpi.compareTo(other.kpi);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.date.write(out);
        this.platForm.write(out);
        this.kpi.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date.readFields(in);
        this.platForm.readFields(in);
        this.kpi.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StatsCommonDimension that = (StatsCommonDimension) o;

        if (date != null ? !date.equals(that.date) : that.date != null) {
            return false;
        }
        if (platForm != null ? !platForm.equals(that.platForm) : that.platForm != null) {
            return false;
        }
        return kpi != null ? kpi.equals(that.kpi) : that.kpi == null;
    }

    @Override
    public int hashCode() {
        int result = date != null ? date.hashCode() : 0;
        result = 31 * result + (platForm != null ? platForm.hashCode() : 0);
        result = 31 * result + (kpi != null ? kpi.hashCode() : 0);
        return result;
    }
}
