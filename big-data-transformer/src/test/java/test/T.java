package test;

import com.wyu.etl.util.IPSeekerExt;
import com.wyu.etl.util.IpUtil;
import com.wyu.etl.util.UserAnentUtil;
import com.wyu.etl.util.ip.IpInfo;
import org.junit.Test;

import java.util.List;

/**
 * 测试类
 *
 * @author:Ken
 */
public class T {

    @Test
    public void example() {
        IPSeekerExt ipSeekerExt = new IPSeekerExt();
        System.out.println(ipSeekerExt.analyticIp("202.192.240.126"));
    }

    @Test
    public void testGetIpInfos() throws Exception {
        List<IpInfo> infos = IpUtil.getIpInfos();
        for (IpInfo info : infos) {
            System.out.println(info);
        }
        System.out.println(infos.size());
    }

    @Test
    public void testAgent() {
        System.out.println(UserAnentUtil.analyticUserAgent("Mozilla/5.0 (Windows NT 6.1) App;eWebKit/537.36 (KHTML,like Gecko) Chrome/61.0.3163.100 Safari/537.36"));

    }
}
