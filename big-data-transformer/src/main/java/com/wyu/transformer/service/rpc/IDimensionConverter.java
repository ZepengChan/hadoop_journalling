package com.wyu.transformer.service.rpc;

import com.wyu.transformer.model.dim.base.BaseDimension;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * 专门操作dimension表的接口
 *
 * @author ken
 */
public interface IDimensionConverter extends VersionedProtocol{
    long versionId = 1;

    /**
     * 根据dimension的value获取id
     * 如果数据库中有，那么直接返回。如果没有，那么先插入再获取id返回
     * @param dimension
     * @return
     * @throws IOException
     */
    int getDimensionIdByValue(BaseDimension dimension) throws IOException;
}
