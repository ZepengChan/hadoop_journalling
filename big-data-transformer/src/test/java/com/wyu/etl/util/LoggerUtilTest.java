package com.wyu.etl.util;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class LoggerUtilTest {
    @Test
    public void handleLog() throws Exception {

        String log = "192.168.56.1^A1510907627.587^A192.168.56.131^A/BfImg.gif?en=e_e&ca=event%E7%9A%84category%E5%90%8D%E7%A7%B0&ac=event%E7%9A%84action%E5%90%8D%E7%A7%B0&kv_key1=value1&kv_key2=value2&du=1245&ver=1&pl=website&sdk=js&u_ud=182CCB47-7A38-4CFC-833A-3DA9F99606C9&u_mid=gerryliu&u_sd=64A42371-C410-4662-BC6B-A192FB599C45&c_time=1510907627644&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%2010.0%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F64.0.3253.3%20Safari%2F537.36&b_rst=1920*1080\n";
        Map map = LoggerUtil.handleLog(log);
        System.out.println(map);
        System.out.println(map.get("province"));
        System.out.println(map.get("city"));
    }

}