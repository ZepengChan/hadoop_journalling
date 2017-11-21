package com.wyu.transformer.service;

import com.wyu.transformer.model.dim.base.BaseDimension;

import java.io.IOException;

/**
 * 专门操作dimension表的接口
 *
 * @author ken
 */
public interface IDimensionConverter {

    /**
     * 根据dimension的value获取id
     * 如果数据库中有，那么直接返回。如果没有，那么先插入再获取id返回
     * @param dimension
     * @return
     * @throws IOException
     */
    int getDimensionIdByValue(BaseDimension dimension) throws IOException;
}
