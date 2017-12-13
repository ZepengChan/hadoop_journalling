package com.wyu.ae.service;

import com.wyu.ae.model.QueryModel;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;



/**
 * 处理ae基本数据交换的接口
 * 
 * @author ken
 *
 */
public interface AEService {
    List<Map<String, Object>> execute(QueryModel queryModel) throws Exception;

}
