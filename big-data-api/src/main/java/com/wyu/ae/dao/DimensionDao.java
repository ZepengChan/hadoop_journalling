package com.wyu.ae.dao;

import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

public interface DimensionDao {

    List<Map<String, Object>> queryDimensionData(Map<String, String> queryMap);
}
