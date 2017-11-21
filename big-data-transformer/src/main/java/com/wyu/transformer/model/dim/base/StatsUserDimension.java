package com.wyu.transformer.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用户分析（用户基本分析和浏览器分析）组合维度
 * @author ken
 */
public class StatsUserDimension extends StatsDimension {

    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    private BrowserDimension browser = new BrowserDimension();

    public StatsUserDimension(StatsCommonDimension statsCommon, BrowserDimension browser) {
        this.statsCommon = statsCommon;
        this.browser = browser;
    }

    public StatsUserDimension() {
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public BrowserDimension getBrowser() {
        return browser;
    }

    public void setBrowser(BrowserDimension browser) {
        this.browser = browser;
    }

    /**
     * 克隆一个实例对象
     *
     * @param dimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension dimension) {
        StatsCommonDimension statsCommon = StatsCommonDimension.clone(dimension.statsCommon);
        BrowserDimension browser = new BrowserDimension(dimension.browser.getId(),dimension.browser.getBrowserName(),dimension.browser.getBrowserVersion());
        return new StatsUserDimension(statsCommon,browser);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o) {
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommon.compareTo(other.statsCommon);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.browser.compareTo(other.browser);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommon.write(out);
        this.browser.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommon.readFields(in);
        this.browser.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StatsUserDimension that = (StatsUserDimension) o;

        if (statsCommon != null ? !statsCommon.equals(that.statsCommon) : that.statsCommon != null) {
            return false;
        }
        return browser != null ? browser.equals(that.browser) : that.browser == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommon != null ? statsCommon.hashCode() : 0;
        result = 31 * result + (browser != null ? browser.hashCode() : 0);
        return result;
    }
}
