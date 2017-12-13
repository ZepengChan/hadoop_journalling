package com.wyu.ae.service;

import com.wyu.ae.model.PlatformDimension;

import java.util.List;
import java.util.Map;

public interface DimensionService {
    List<Map<String, Object>> queryDimensionData(Map<String, String> queryMap);

    PlatformDimension getPlatformDimension(int dimensionPlatformId);

    PlatformDimension getPlatformDimension(String platformName);

    Integer getKpiDimensionId(String kpiName);

    Integer getDateDimensionId(int year, int season, int month, int week, int day);
    
//    public BrowserDimension getBrowserDimension(int browserId);
//    
//    public BrowserDimension getBrowserDimension(String browser, String browser_version);
//
//    public OsDimension getOsDimension(int dimensionOsId);
//
//    public OsDimension getOsDimension(String osName, String osVersion);
//
//    public LocationDimension getLocationDimension(int dimensionLocationId);
//
//    public LocationDimension getLocationDimension(String country, String province, String city);
//
//    public InboundDimension getInboundDimension(int dimensionInboundId);
//
//    public InboundDimension getInboundDimension(String name);
//
//    
//    public KeywordDimension getKeywordDimension(int keywordId);
//    
//    public KeywordDimension getKeywordDimension(String keyword);
}