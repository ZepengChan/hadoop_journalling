package com.wyu.transformer.util;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 操作member_info表的工具类,主要作用的判断member_id是否是一个正常的id以及是否是一个新的会员id
 *
 * @author ken
 * @date 2017/11/27
 */
public class MemberUtil {

    private static Map<String,Boolean> cache = new LinkedHashMap<String, Boolean>(){

        @Override
        protected boolean removeEldestEntry(Entry<String, Boolean> eldest) {
            //最多保存1W个数据
            return this.size()> 10000;
        }
    };

    /**
     * 删除指定日期的数据
     * 
     * @param date
     * @param connection
     * @throws SQLException
     */
    public static void deleteMemberInfoByDate(String date, Connection connection) throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM `member_info` WHERE `created` = ?");
            pstmt.setString(1, date);
            pstmt.execute();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception e) {
                    // nothing
                }
            }
        }

    }
    /**
     * p判断memberId的格式是否有效
     *
     * @param memberId
     * @return
     */
    public static boolean isValidateMemberId(String memberId) {
        return StringUtils.isNotBlank(memberId) && memberId.trim().matches("[0-9a-zA-z]{1,32}");
    }

    /**
     * 判断memberId是否为一个新的id
     *
     * @param memberId
     * @param conn     数据库连接对象
     * @return
     */
    public static boolean isNewMemberId(String memberId, Connection conn) throws SQLException {
        Boolean isNewMemberId = null;
        if(StringUtils.isNotBlank(memberId)) {
            //要求memberid不为空
            isNewMemberId = cache.get(memberId);
            if(isNewMemberId == null) {
                PreparedStatement psmt = null;
                ResultSet rs = null;
                try {
                    psmt = conn.prepareStatement("SELECT  `member_id`, `last_visit_date` FROM `member_info` WHERE `member_id` = ?");
                    psmt.setString(1, memberId);
                    rs = psmt.executeQuery();
                    if (rs.next()) {
                        //数据库中存在memberid,则不是 新的会员
                        isNewMemberId = Boolean.FALSE;
                    } else {
                        //数据库没有存在memberid,则是新的会员
                        isNewMemberId = Boolean.TRUE;
                    }
                    cache.put(memberId,isNewMemberId);

                } finally {
                    try {
                        if (rs != null) {
                            rs.close();
                        }
                        if (psmt != null) {
                            psmt.close();
                        }
                    } catch (SQLException e) {
                        //nothing
                    }
                }
            }

        }
        return isNewMemberId==null ? false : isNewMemberId;
    }
}
