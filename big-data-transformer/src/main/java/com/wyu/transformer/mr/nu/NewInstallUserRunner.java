package com.wyu.transformer.mr.nu;

import com.wyu.commom.DateEnum;
import com.wyu.commom.EventLogConstants;
import com.wyu.commom.EventLogConstants.EventEnum;
import com.wyu.commom.GlobalConstants;
import com.wyu.transformer.model.dim.base.DateDimension;
import com.wyu.transformer.model.dim.base.StatsUserDimension;
import com.wyu.transformer.model.value.map.TimeOutputValue;
import com.wyu.transformer.model.value.reduce.MapWritableValue;
import com.wyu.transformer.mr.TransformerBaseRunner;
import com.wyu.util.JdbcManager;
import com.wyu.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ken
 * @date 2017/11/23
 */
public class NewInstallUserRunner extends TransformerBaseRunner {

    private static final Logger logger = Logger.getLogger(NewInstallUserRunner.class);

    public static void main(String[] args) {
        NewInstallUserRunner runner = new NewInstallUserRunner();
        runner.setupRunner("new_installUser",NewInstallUserRunner.class,NewInstallUserMapper.class,NewInstallUserReducer.class,StatsUserDimension.class, TimeOutputValue.class, StatsUserDimension.class, MapWritableValue.class);

        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("运行new_installUser任务出现异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void afterRunJob(Job job, Throwable error) throws IOException {
        if(job.isSuccessful()) {
            calculateTotalUsers(job.getConfiguration());
        }
        super.afterRunJob(job, error);
    }


    private void calculateTotalUsers(Configuration conf) {
        Connection conn = null;
        PreparedStatement psmt = null;
        ResultSet rs = null;
        long date = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        /*获取今天的dimension*/
        DateDimension todayDimension = DateDimension.buildDate(date, DateEnum.DAY);
        /*获取昨天的dimension*/
        DateDimension yesterdayDimension = DateDimension.buildDate(date - GlobalConstants.DAY_OF_MILLISECONDS, DateEnum.DAY);
        int yesterdayDimensionId = -1;
        int todayDimensionId = -1;

        try {
            /*获取执行时间昨天的id*/
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            psmt = conn.prepareStatement(" SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ? ");
            int i = 0;
            psmt.setInt(++i, yesterdayDimension.getYear());
            psmt.setInt(++i, yesterdayDimension.getSeason());
            psmt.setInt(++i, yesterdayDimension.getMonth());
            psmt.setInt(++i, yesterdayDimension.getWeek());
            psmt.setInt(++i, yesterdayDimension.getDay());
            psmt.setString(++i, yesterdayDimension.getType());
            psmt.setDate(++i, new Date(yesterdayDimension.getCalendar().getTime()));
            rs = psmt.executeQuery();
            if (rs.next()) {
                yesterdayDimensionId = rs.getInt(1);

            }

            /*获取执行时间当天的id*/
            psmt = conn.prepareStatement(" SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ? ");
            i = 0;
            psmt.setInt(++i, todayDimension.getYear());
            psmt.setInt(++i, todayDimension.getSeason());
            psmt.setInt(++i, todayDimension.getMonth());
            psmt.setInt(++i, todayDimension.getWeek());
            psmt.setInt(++i, todayDimension.getDay());
            psmt.setString(++i, todayDimension.getType());
            psmt.setDate(++i, new Date(todayDimension.getCalendar().getTime()));
            rs = psmt.executeQuery();
            if (rs.next()) {
                todayDimensionId = rs.getInt(1);

            }
              /*获取昨天的原始数据,存储格式为: dateid_platformid=totalusers */
            Map<String, Integer> oldValueMap = new HashMap<>();
            if(yesterdayDimensionId > -1){
                psmt = conn.prepareStatement("select `platform_dimension_id`,`total_install_users` from `stats_user` where `date_dimension_id`=?");
                psmt.setInt(1, yesterdayDimensionId);
                rs = psmt.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUsers = rs.getInt("total_install_users");
                    oldValueMap.put("" + platformId, totalUsers);
                }
            }
            // 添加今天的总用户
            psmt = conn.prepareStatement("select `platform_dimension_id`,`new_install_users` from `stats_user` where `date_dimension_id`=?");
            psmt.setInt(1, todayDimensionId);
            rs = psmt.executeQuery();
            while (rs.next()) {
                int platformId = rs.getInt("platform_dimension_id");
                int newUsers = rs.getInt("new_install_users");
                if (oldValueMap.containsKey("" + platformId)) {
                    newUsers += oldValueMap.get("" + platformId);
                }
                oldValueMap.put("" + platformId, newUsers);
            }

            // 更新操作
            psmt = conn.prepareStatement("INSERT INTO `stats_user`(`platform_dimension_id`,`date_dimension_id`,`total_install_users`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE `total_install_users` = ?");
            for (Map.Entry<String, Integer> entry : oldValueMap.entrySet()) {
                psmt.setInt(1, Integer.valueOf(entry.getKey()));
                psmt.setInt(2, todayDimensionId);
                psmt.setInt(3, entry.getValue());
                psmt.setInt(4, entry.getValue());
                psmt.execute();
            }

            // 开始更新stats_device_browser
            oldValueMap.clear();
            if (yesterdayDimensionId > -1) {
                psmt = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`total_install_users` from `stats_device_browser` where `date_dimension_id`=?");
                psmt.setInt(1, yesterdayDimensionId);
                rs = psmt.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int browserId = rs.getInt("browser_dimension_id");
                    int totalUsers = rs.getInt("total_install_users");
                    oldValueMap.put(platformId + "_" + browserId, totalUsers);
                }
            }

            // 添加今天的总用户
            psmt = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`new_install_users` from `stats_device_browser` where `date_dimension_id`=?");
            psmt.setInt(1, todayDimensionId);
            rs = psmt.executeQuery();
            while (rs.next()) {
                int platformId = rs.getInt("platform_dimension_id");
                int browserId = rs.getInt("browser_dimension_id");
                int newUsers = rs.getInt("new_install_users");
                String key = platformId + "_" + browserId;
                if (oldValueMap.containsKey(key)) {
                    newUsers += oldValueMap.get(key);
                }
                oldValueMap.put(key, newUsers);
            }

            // 更新操作
            psmt = conn.prepareStatement("INSERT INTO `stats_device_browser`(`platform_dimension_id`,`browser_dimension_id`,`date_dimension_id`,`total_install_users`) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `total_install_users` = ?");
            for (Map.Entry<String, Integer> entry : oldValueMap.entrySet()) {
                String[] key = entry.getKey().split("_");
                psmt.setInt(1, Integer.valueOf(key[0]));
                psmt.setInt(2, Integer.valueOf(key[1]));
                psmt.setInt(3, todayDimensionId);
                psmt.setInt(4, entry.getValue());
                psmt.setInt(5, entry.getValue());
                psmt.execute();
            }
        } catch (Exception e) {
            logger.error("执行统计总用户数发生异常!" + e);
            throw  new RuntimeException("执行统计总用户数发生异常!",e);

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (psmt != null) {
                    psmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                //nothing
            }
        }
    }

    @Override
    protected Filter fetchHbaseFilter() {
        FilterList filterList = new FilterList();
        // 定义mapper中需要获取的列名
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
                EventLogConstants.LOG_COLUMN_NAME_UUID,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PALTFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION
        };
        /*过滤数据.只分析launch事件*/
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(EventEnum.LAUNCH.alias)));
        filterList.addFilter(this.getColumnFilter(columns));
        return filterList;
    }


}
