package com.wyu.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;

import java.io.IOException;

/**
 * 解析浏览器工具类，内部就是调用UASparser jar
 *
 * @author:Ken
 */
public class UserAnentUtil {

    static UASparser uaSparser = null;

    //静态代码块,初始化对象
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析浏览器的userAgent字符串，返回UserAgentInfo对象
     * 解析失败（异常）直接返回null
     *
     * @param userAgent 字符串
     * @return
     */
    public static UserAgentInfo analyticUserAgent(String userAgent) {
        cz.mallat.uasparser.UserAgentInfo info = null;
        UserAgentInfo result = null;
        if (!(userAgent == null || userAgent.trim().isEmpty())) {
            try {
                info = uaSparser.parse(userAgent);
                result = new UserAgentInfo();
                result.setBrowserName(info.getUaFamily());
                result.setBrowserVersion(info.getBrowserVersionInfo());
                result.setOsName(info.getOsFamily());
                result.setOsVersion(info.getOsName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 内部解析后浏览器信息的model对象
     */
    public static class UserAgentInfo {
        private String browserName; //浏览器名称
        private String browserVersion;  //浏览器版本
        private String osName; //操作系统名称
        private String osVersion; //操作系统版本

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        public String getOsName() {

            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getBrowserVersion() {

            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}
