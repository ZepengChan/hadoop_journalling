package com.wyu.ae.dao.impl;


import com.wyu.ae.dao.KpiDimensionDao;
import com.wyu.ae.dao.mybatis.BaseDao;
import com.wyu.ae.model.KpiDimension;
import org.springframework.stereotype.Repository;

@Repository
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