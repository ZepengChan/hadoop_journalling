package com.beifeng.ae.dao.impl;

import com.beifeng.ae.dao.PlatformDimensionDao;
import com.beifeng.ae.dao.mybatis.BaseDao;
import com.beifeng.ae.model.PlatformDimension;

public class PlatformDimensionDaoImpl extends BaseDao implements PlatformDimensionDao {

    private static String modelClass = PlatformDimension.class.getName();
    private static String getPlatformDimension = modelClass + ".getPlatformDimension";

    @Override
    public PlatformDimension getPlatformDimension(PlatformDimension platform) {
        return this.getSqlSession().selectOne(getPlatformDimension, platform);
    }

    @Override
    public PlatformDimension getPlatformDimension(String platform) {
        PlatformDimension plat = new PlatformDimension(platform);
        return getPlatformDimension(plat);
    }

    @Override
    public PlatformDimension getPlatformDimension(int id) {
        PlatformDimension plat = new PlatformDimension(id);
        return getPlatformDimension(plat);
    }

}