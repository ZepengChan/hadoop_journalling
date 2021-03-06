package com.wyu.transformer.model.dim.base;

import com.wyu.commom.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 平台维度类
 *
 * @author ken
 */
public class PlatformDimension extends BaseDimension {

    private int id;
    private String platformName;

    public PlatformDimension() {
    }


    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id, String platformName) {
        this.id = id;
        this.platformName = platformName;
    }

    public static List<PlatformDimension> buildList(String platformName) {
        if (StringUtils.isBlank(platformName)) {
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        List<PlatformDimension> list = new ArrayList<>();
//        list.add(new PlatformDimension((GlobalConstants.DEFAULT_VALUE)));
        list.add(new PlatformDimension(platformName));
        return list;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        PlatformDimension other = (PlatformDimension) o;
        int tmp = Integer.compare(this.id, other.id);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.platformName.compareTo(other.platformName);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.platformName = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PlatformDimension that = (PlatformDimension) o;

        return id == that.id && (platformName != null ? platformName.equals(that.platformName) : that.platformName == null);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (platformName != null ? platformName.hashCode() : 0);
        return result;
    }
}
