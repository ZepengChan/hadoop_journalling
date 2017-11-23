package com.wyu.commom;

/**
 * 全局常量类
 *
 * @author gerry
 */
public class GlobalConstants {

    /**
     * 一天的毫秒数
     */
    public static final int DAY_OF_MILLISECONDS = 86400000;
    /**
     * 定义的运行时间变量名
     */
    public static final String RUNNING_DATE_PARAMES = "RUNNING_DATE";

    /**
     * 默认值
     */
    public static final String DEFAULT_VALUE = "unkonwn";

    /**
     * 指定全部列值
     */
    public static final String VALUE_OF_ALL = "all";

    /**
     * 定义的outp collector的前缀
     */
    public static final String OUTPUT_COLLECTOR_KEY_PREFIX = "collector_";

    /**
     * 指定连接表
     */
    public static final String WAREHOUSE_OF_REPORT = "report";

    /**
     * 批量执行的key
     */
    public static final String JDBC_BATCH_NUMBER = "mysql.batch.number";
    /**
     * 默认key的大小
     */
    public static final String DEFAULT_JDBC_BATCH_NUMBER = "500";

    /**
     * driver 名称
     */
    public static final String JDBC_DRIVER = "mysql.%s.driver";

    /**
     * jdbc url
     */
    public static final String JDBC_URL = "mysql.%s.url";
    /**
     * jdbc username
     */
    public static final String JDBC_USERNAME = "mysql.%s.username";

    /**
     * jdbc password
     */
    public static final String JDBC_PASSWORD = "mysql.%s.password";
}
