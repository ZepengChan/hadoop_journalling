package com.wyu.etl.util;

import com.wyu.etl.util.file.FileUtil;
import com.wyu.etl.util.file.PoiUtil;
import com.wyu.etl.util.ip.IpInfo;
import com.wyu.etl.util.ip.IpTree;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对外的 ip地址工具类，调用解析类的方法
 */
public class IpUtil {

    /**
     * 调用IpHelper类解析ip地址，返回自定义对象IpInfo
     *
     * @param ip ip字符串
     * @return
     */
    public static IpInfo analyticIp(String ip) {
        IpInfo info = new IpInfo();
        String[] area = IpHelper.findRegionByIp(ip).split(",");
        if(!"全球".equals(area[0])){
            info.setCountry("中国");

        }else if ("全球".equals(area[1])){
            info.setCountry("全球");
        }
        info.setProvince(area[0]);
        info.setCity(area[1]);
        System.out.println(area.length);
        return info;
    }

    /**
     * 解析一个ip集合 返回ipInfo集合
     *
     * @param ips
     * @return
     */
    public static List<IpInfo> analyticIp(List<String> ips) {
        List<IpInfo> infos = new ArrayList<>();
        /*
         * 循环调用解析单个ip的方法
         */
        for (String ip : ips) {
            IpInfo info = analyticIp(ip);
            infos.add(info);
        }
        return infos;
    }

    /**
     * 查找所有的省份城市信息并返回，实际上是调用解析类中的方法
     *
     * @return IpInfo的List集合
     * @throws Exception
     */
    public static List<IpInfo> getIpInfos() throws Exception {
        return IpHelper.getIpRelation();
    }


    /**
     * ip地址解析类 这里只对工具类开放方法，内部类私有化
     * 构建二查找树，查找时间复杂度 log2n
     * 由于需要打开文件，查找少数的时候，第一次会比较慢
     */
    private static class IpHelper {

        private static final String IP_FILE = "ipDatabase.csv";
        private static final String regionFile = "ipRegion.xlsx";
        private static IpTree ipTree = IpTree.getInstance();

        //静态块 每次加载类的时候就构建二叉树
        static {
            buildTrain();
        }

        //构建二叉树，调用二叉树类中的构建方法
        private static void buildTrain() {
            List<com.wyu.etl.util.ip.IpInfo> ipInfoList;
            try {
                ipInfoList = getIpRelation();
                for (IpInfo ipInfo : ipInfoList) {
                    /*
                     * 这里为了方便构建，把省份+","+城市一起传递进去，中间用逗号隔开，方便后续操作
                     */
                    ipTree.train(ipInfo.getIpStart(), ipInfo.getIpEnd(), ipInfo.getProvince() + "," + ipInfo.getCity());

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * 静态方法，传入ip地址，返回ip地址所在省份+城市
         *
         * @param ip IP地址，例：58.30.15.255
         * @return 返回IP地址所在城市或地区，例：北京市北京市
         */
        private static String findRegionByIp(String ip) {
            return ipTree.findIp(ip);
        }

        /**
         * 返回所有的地域信息IpRelation
         *
         * @return
         * @throws Exception
         */
        private static List<IpInfo> getIpRelation() throws Exception {

            // <ipCode, province+","+city>
            Map<Integer, String> regionRelationMap = getRegionRelationMap();
            String file = IpHelper.class.getClassLoader().getResource(IP_FILE).getFile();
            BufferedReader ipRelationReader = FileUtil.readFile(file);

            String line;
            List<IpInfo> list = new ArrayList<>();
            while ((line = ipRelationReader.readLine()) != null) {
                String[] split = line.split(",");
                String ipStart = split[0];
                String ipEnd = split[1];
                Integer ipCode = Integer.valueOf(split[2]);

                String[] areas = regionRelationMap.get(ipCode).split(",");
                IpInfo ipInfo = new IpInfo();
                ipInfo.setIpStart(ipStart);
                ipInfo.setIpEnd(ipEnd);
                ipInfo.setProvince(areas[0]);
                ipInfo.setCity(areas[1]);
                if (!"全球".equals(ipInfo.getProvince())){
                    ipInfo.setCountry("中国");
                }else{
                    ipInfo.setCountry("全球");
                }
                list.add(ipInfo);
            }
            return list;

        }

        /**
         * 表格操作方法
         *
         * @return Map<ipCode, province> 返回map key为城市代码 value为province+city
         * @throws Exception
         */
        private static Map<Integer, String> getRegionRelationMap() throws Exception {
            String file = IpHelper.class.getClassLoader().getResource(regionFile).getFile();

            Workbook workbook = PoiUtil.getWorkbook(file);

            Sheet sheet = workbook.getSheetAt(0);
            Map<Integer, String> map = new HashMap<Integer, String>(16);
            int rowLen = sheet.getPhysicalNumberOfRows();
            //总共有三行
            for (int i = 1; i < rowLen; i++) {
                Row row = sheet.getRow(i);
                String province = row.getCell(0).getStringCellValue() + "," + row.getCell(1).getStringCellValue();
                Double a = row.getCell(2).getNumericCellValue();
                Integer ipCode = a.intValue();
                map.put(ipCode, province);
            }

            return map;
        }
    }

}

/**
 * 以下是
 * 联网调用淘宝ip地址库解析的类，缺点就是当需要解析大量ip的时候耗资源要多
 * 也容易出现socket异常
 * <p>
 * <p>
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 *
 * @param ip ip字符串
 * @return 解析多个ip返回多个ipInfo
 * @param ips ip地址的List集合
 * @return 将字符串json转成json对象
 * @param json 字符串的json
 * @return ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 * <p>
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 * @param ip ip字符串
 * @return 解析多个ip返回多个ipInfo
 * @param ips ip地址的List集合
 * @return 将字符串json转成json对象
 * @param json 字符串的json
 * @return ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 * <p>
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 * @param ip ip字符串
 * @return 解析多个ip返回多个ipInfo
 * @param ips ip地址的List集合
 * @return 将字符串json转成json对象
 * @param json 字符串的json
 * @return ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 * <p>
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 * @param ip ip字符串
 * @return 解析多个ip返回多个ipInfo
 * @param ips ip地址的List集合
 * @return 将字符串json转成json对象
 * @param json 字符串的json
 * @return ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 * <p>
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 * @param ip ip字符串
 * @return 解析多个ip返回多个ipInfo
 * @param ips ip地址的List集合
 * @return 将字符串json转成json对象
 * @param json 字符串的json
 * @return ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 * <p>
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 * @param ip ip字符串
 * @return 解析多个ip返回多个ipInfo
 * @param ips ip地址的List集合
 * @return 将字符串json转成json对象
 * @param json 字符串的json
 * @return ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 * <p>
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 * @param ip ip字符串
 * @return 解析多个ip返回多个ipInfo
 * @param ips ip地址的List集合
 * @return 将字符串json转成json对象
 * @param json 字符串的json
 * @return ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 *//*

public class IpUtil {


    //淘宝ip地址库
    public static final String IP_URL = "http://ip.taobao.com/service/getIpInfo.php?ip=";

    */
/**
 * 真正解析ip的方法
 * 返回ipinfo对象
 * 若解析异常直接返回默认数据 unknown
 *
 * @param ip ip字符串
 * @return
 *//*

    public static IpInfo analyticIp(String ip) {
        IpInfo info = new IpInfo();
        JsonObject retJo = null;
        try {

            retJo = parseString2Json(post(ip)); //转换成json对象
            if (retJo.get("code").getAsInt() == 0) {
                retJo = retJo.get("data").getAsJsonObject();
                String[] areas = new String[3];
                areas[0] = retJo.get("country").getAsString();
                areas[1] = retJo.get("region").getAsString();
                areas[2] = retJo.get("city").getAsString();

                //以下判断获取到的数据不为空时才赋值，否则为unknown
                if (!areas[0].trim().isEmpty() && areas[0] != null) {
                    info.setCountry(retJo.get("country").getAsString());
                }
                if (!areas[1].trim().isEmpty() && areas[1] != null) {
                    info.setProvince(retJo.get("region").getAsString());
                }
                if (!areas[2].trim().isEmpty() && areas[2] != null) {
                    info.setCity(retJo.get("city").getAsString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return info;
    }

    */
/**
 * 解析多个ip返回多个ipInfo
 *
 * @param ips ip地址的List集合
 * @return
 *//*

    public static List<IpInfo> analyticIp(List<String> ips) {
        List<IpInfo> infos = new ArrayList<>();
        IpInfo info = null;
        for (String ip : ips) {
           info = analyticIp(ip);
           infos.add(info);

        }
        return infos;
    }

    private static String post(String ip) throws Exception {
        HttpURLConnection con = null;
        BufferedReader in = null;
        StringBuilder sb = new StringBuilder(IP_URL);
        sb.append(ip);
        try {
            URL url = new URL(sb.toString());
            con = (HttpURLConnection) url.openConnection(); // 打开url链接
            // 设置参数
            con.setConnectTimeout(2000); // 连接过期时间
            con.setReadTimeout(2000); // 读取过期时间
            con.setRequestMethod("GET"); // 请求类型为get
            in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String line = null;
            sb.delete(0, sb.length());//删除字符串缓存
            while ((line = in.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            if (in != null) {
                in.close();
            }
            if (con != null) {
                con.disconnect();
            }
        }
        return sb.toString();
    }

    */
/**
 * 将字符串json转成json对象
 *
 * @param json 字符串的json
 * @return
 *//*

    private static JsonObject parseString2Json(String json) {
        JsonObject retJo = null;
        JsonParser jsonParser = new JsonParser();
        retJo = jsonParser.parse(json).getAsJsonObject();
        return retJo;
    }

    */
/**
 * ip地址信息的model主要包含
 * 国家 省份 城市 （默认都是unknown）
 *//*

    public static class IpInfo {
        public static final String DEFAULT_VALUE = "unknow";
        private String country = DEFAULT_VALUE;
        private String province = DEFAULT_VALUE;
        private String city = DEFAULT_VALUE;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "IpInfo{" +
                    "country='" + country + '\'' +
                    ", province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }

}
*/
