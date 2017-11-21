package com.wyu.transformer.model.value;

import com.wyu.commom.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * 自定义顶级输出value
 * @author ken
 */
public  abstract class BaseStatsValueWritable  implements Writable{

    /**
     * 获取当前value对应的kpi
     * @return
     */
    public abstract KpiType getKpi();
}
