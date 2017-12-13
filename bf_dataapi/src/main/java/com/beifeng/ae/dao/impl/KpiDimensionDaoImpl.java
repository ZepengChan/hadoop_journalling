package com.beifeng.ae.dao.impl;

import com.beifeng.ae.dao.KpiDimensionDao;
import com.beifeng.ae.dao.mybatis.BaseDao;
import com.beifeng.ae.model.KpiDimension;

public class KpiDimensionDaoImpl extends BaseDao implements KpiDimensionDao {

    private static String modelClass = KpiDimension.class.getName();
    private static String getKpiDimension = modelClass + ".getKpiDimension";

    @Override
    public KpiDimension getKpiDimension(KpiDimension kpi) {
        return this.getSqlSession().selectOne(getKpiDimension, kpi);
    }

    @Override
    public KpiDimension getKpiDimension(String name) {
        KpiDimension kpi = new KpiDimension(name);
        return getKpiDimension(kpi);
    }

    @Override
    public KpiDimension getKpiDimension(int id) {
        KpiDimension kpi = new KpiDimension(id);
        return getKpiDimension(kpi);
    }

}