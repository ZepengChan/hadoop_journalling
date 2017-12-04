package com.wyu.transformer.model.dim;

import com.wyu.transformer.model.dim.base.BaseDimension;
import com.wyu.transformer.model.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 统计地域信息的相关维度类
 *
 * @author ken
 * @date 2017/12/4
 */
public class StatsLocationDimension extends StatsDimension {
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    private LocationDimension location = new LocationDimension();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(StatsCommonDimension statsCommon, LocationDimension location) {
        this.statsCommon = statsCommon;
        this.location = location;
    }

    /**
     * 克隆现有的对象
     * @param dimension
     * @return
     */
    public static StatsLocationDimension clone(StatsLocationDimension dimension) {
        StatsLocationDimension newDimension = new StatsLocationDimension();
        newDimension.statsCommon = StatsCommonDimension.clone(dimension.statsCommon);
        newDimension.location = LocationDimension.newInstance(dimension.location.getCountry(), dimension.location.getProvince(), dimension.location.getCity());
        newDimension.location.setId(dimension.location.getId());
        return newDimension;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public LocationDimension getLocation() {
        return location;
    }

    public void setLocation(LocationDimension location) {
        this.location = location;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }
        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.statsCommon.compareTo(other.statsCommon);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.location.compareTo(other.location);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.location.write(out);
        this.statsCommon.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.location.readFields(in);
        this.statsCommon.readFields(in);
    }
}
