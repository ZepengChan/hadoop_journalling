package com.wyu.commom;

/**
 * 统计kpi的枚举
 * @author ken
 */
public enum KpiType {
    NEW_INSTALL_USER("new_install_user"),
    BROWSER_NEW_INSTALL_USER("browser_new_install_user");

    public final String name;

    KpiType(String name) {
        this.name = name;
    }
}
