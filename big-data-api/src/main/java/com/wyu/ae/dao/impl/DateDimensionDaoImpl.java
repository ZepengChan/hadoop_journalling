package com.wyu.ae.dao.impl;


import com.wyu.ae.dao.DateDimensionDao;
import com.wyu.ae.dao.mybatis.BaseDao;
import com.wyu.ae.model.DateDimension;
import org.springframework.stereotype.Repository;

@Repository
public class DateDimensionDaoImpl extends BaseDao implements DateDimensionDao {

    private static String modelClass = DateDimension.class.getName();
    private static String getDateDimensionId = modelClass + ".getDateDimensionId";

    public Integer getDateDimensionId(DateDimension date) {
        return this.getSqlSession().selectOne(getDateDimensionId, date);
    }

    @Override
    public Integer getDateDimensionId(int year, int season, int month, int week, int day) {
        DateDimension date = new DateDimension(year, season, month, week, day);
        return getDateDimensionId(date);
    }

}