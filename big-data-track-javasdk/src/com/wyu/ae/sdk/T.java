package com.wyu.ae.sdk;

public class T {

    public static void main(String[] args) {
        AnalyticsEngineSdk.onChargeRefund("orderid123", "ken123");
        AnalyticsEngineSdk.onChargeRefund("orderid123456", "ken123456");
        AnalyticsEngineSdk.onChargeSuccess("orderid123", "ken123");
        AnalyticsEngineSdk.onChargeSuccess(System.currentTimeMillis() + "", "yuhe3929");
    }

}
