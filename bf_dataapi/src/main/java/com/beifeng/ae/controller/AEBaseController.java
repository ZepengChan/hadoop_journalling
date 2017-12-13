package com.beifeng.ae.controller;

import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import com.beifeng.ae.common.AEConstants;
import com.beifeng.ae.model.Message;
import com.beifeng.ae.model.Message.MessageEntry;
import com.beifeng.ae.model.PlatformDimension;
import com.beifeng.ae.model.QueryColumn;
import com.beifeng.ae.service.AEService;
import com.beifeng.ae.service.DimensionService;

/**
 * 基础controller类
 * 
 * @author gerry
 *
 */
public abstract class AEBaseController {
    private static final Logger log = Logger.getLogger(AEBaseController.class);
    // bucket和metric的映射关系
    @Resource
    protected Map<String, Set<Object>> bucketMetrics;
    // bucket支持group by的列名
    @Resource
    protected Map<String, Set<Object>> groupByColumns;
    // bucket&metric和queryid的映射关系
    @Resource
    protected Map<String, String> bucketMetricQueryId;
    // bucket&metric和返回列的映射关系
    @Resource
    protected Map<String, String> bucketMetricColumns;

    @Resource(name = "aeService")
    protected AEService aeService;

    @Resource(name = "dimensionService")
    protected DimensionService dimensionService;

    /**
     * 全局的处理异常方法
     * 
     * @param e
     * @return
     */
    @ExceptionHandler(Throwable.class)
    @ResponseBody
    public MessageEntry handleThrowable(Throwable e) {
        log.error("服务器发现异常", e);
        return Message.error(e.getMessage());
    }

    /**
     * 处理platform相关参数
     * 
     * @param request
     * @param groups
     * @param queryColumn
     * @return
     */
    protected boolean setDimensionPlatformId(HttpServletRequest request, Set<String> groups, QueryColumn queryColumn) {
        String platformId = request.getParameter(AEConstants.DIMENSION_PLATFORM_ID);
        int dimensionId = 0;
        try {
            dimensionId = StringUtils.isBlank(platformId) ? 0 : Integer.valueOf(platformId.trim());
        } catch (Throwable e) {
            dimensionId = 0;
        }

        PlatformDimension dimension = null;
        if (dimensionId != 0) {
            // 从数据库中获取
            dimension = this.dimensionService.getPlatformDimension(dimensionId);
        }

        // 如果获取的值为空，那么读取platform参数，进行解析操作
        if (dimension == null) {
            String platform = request.getParameter(AEConstants.PLATFORM);
            // 判断group by操作
            platform = this.checkGroupByColumn(platform, AEConstants.PLATFORM, groups);
            if (AEConstants.GROUP_BY.equals(platform)) {
                dimensionId = 0;
            } else {
                dimension = this.dimensionService.getPlatformDimension(platform);
                if (null == dimension) {
                    return false;
                }
                dimensionId = dimension.getId();
            }
        }

        queryColumn.setDimensionPlatformId(dimensionId);
        return false;
    }

    /**
     * 前提条件： column为空<br/>
     * 检查是否是group操作，即检查给定的groupBy参数是否在groups中出现，如果出现返回group_by。<br/>
     * 否则返回all<br/>
     * 如果column不为空，直接返回原值
     * 
     * @param column
     * @param groupBy
     * @param groups
     * @return
     */
    protected String checkGroupByColumn(String column, String groupBy, Set<String> groups) {
        if (StringUtils.isBlank(column)) {
            if (groups.isEmpty() || !groups.contains(groupBy)) {
                return AEConstants.ALL;
            }
            return AEConstants.GROUP_BY;
        }
        return column.trim();
    }
}
