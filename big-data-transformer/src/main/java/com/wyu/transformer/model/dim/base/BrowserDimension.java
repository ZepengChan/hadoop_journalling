package com.wyu.transformer.model.dim.base;

import com.wyu.commom.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 浏览器维度信息类
 *
 * @author ken
 */
public class BrowserDimension extends BaseDimension {

    private int id;
    /**
     * 浏览器名称
     */
    private String browserName;
    /**
     * 浏览器版本
     */
    private String browserVersion;

    public BrowserDimension() {
    }

    public BrowserDimension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public BrowserDimension(int id, String browserName, String browserVersion) {
        this(browserName,browserVersion);
        this.id = id;

    }

    public static BrowserDimension newInstance(String browserName, String browserVersion) {
        BrowserDimension browserDimension = new BrowserDimension();
        browserDimension.setBrowserName(browserName);
        browserDimension.setBrowserVersion(browserVersion);
        return browserDimension;
    }



    public static List<BaseDimension> buildList(String browserName, String browserVersion) {
        List<BrowserDimension> list = new ArrayList<>();
        if (StringUtils.isBlank(browserName)) {
            browserName = GlobalConstants.DEFAULT_VALUE;
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isBlank(browserVersion)) {
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
//        list.add(BrowserDimension.newInstance(GlobalConstants.VALUE_OF_ALL,GlobalConstants.VALUE_OF_ALL));
        list.add(BrowserDimension.newInstance(browserName, GlobalConstants.VALUE_OF_ALL));
        list.add(BrowserDimension.newInstance(browserName, browserVersion));
        return null;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.browserName);
        out.writeUTF(this.browserVersion);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readInt();
        this.browserName = in.readUTF();
        this.browserVersion = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {

        if (this == o) {
            return 0;
        }
        BrowserDimension other = (BrowserDimension) o;
        int tmp = Integer.compare(this.id, other.id);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.browserName.compareTo(other.browserName);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.browserVersion.compareTo(other.browserVersion);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BrowserDimension that = (BrowserDimension) o;

        return id == that.id && browserName.equals(that.browserName) && browserVersion.equals(that.browserVersion);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + browserName.hashCode();
        result = 31 * result + browserVersion.hashCode();
        return result;
    }
}
