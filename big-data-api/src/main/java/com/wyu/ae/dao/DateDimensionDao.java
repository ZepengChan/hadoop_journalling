package com.wyu.ae.dao;


import com.wyu.ae.model.DateDimension;
import org.springframework.stereotype.Repository;


public interface DateDimensionDao {
    Integer getDateDimensionId(DateDimension date);

    Integer getDateDimensionId(int year, int season, int month, int week, int day);
}