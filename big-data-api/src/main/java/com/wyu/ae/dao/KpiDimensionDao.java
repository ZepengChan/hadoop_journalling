package com.wyu.ae.dao;

import com.wyu.ae.model.KpiDimension;
import org.springframework.stereotype.Repository;

public interface KpiDimensionDao {
    KpiDimension getKpiDimension(KpiDimension kpi);

    KpiDimension getKpiDimension(String name);

    KpiDimension getKpiDimension(int id);
}