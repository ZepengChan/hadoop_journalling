package com.wyu.ae.sdk;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 分析引擎SDK，用于java服务器端的数据搜集
 * 
 * @ClassName: AnalyticsEngineSDK
 * @author 陈泽鹏
 */
public class AnalyticsEngineSdk {

	/**
	 * 请求的日志对象
	 */
	private static final Logger log = Logger.getGlobal();
	/**
	 * 请求链接
 	 */
	private static final String ACCESS_URL = "http://192.168.56.140/BfImg.gif";
	private static final String PLATFOR_NAME = "java_server";
	private static final String SDK_NAME = "jdk";
	private static final String VERSION = "1";

	/**
	 * 触发订单支付成功时间，发送事件数据到服务器
	 * 
	 * @Title: onChargeSuccess
	 * @param orderId
	 *            订单支付id
	 * @param memberId
	 *            订单支付会员id
	 * @return 如果发送数据成功返回true，否则但会false
	 */
	public static boolean onChargeSuccess(String orderId, String memberId) {
		try {
			if (isEmpty(orderId) || isEmpty(memberId)) {
				log.log(Level.WARNING, "订单id和会员id不能为空！");
				return false;
			}
			// orderid 和memberid不为空
			Map<String, String> data = new HashMap<>(16);
			data.put("u_id", memberId);
			data.put("oid", orderId);
			data.put("c_time", String.valueOf(System.currentTimeMillis()));
			data.put("ver", VERSION);
			data.put("en", "e_cs");
			data.put("pl", PLATFOR_NAME);
			data.put("sdk", SDK_NAME);
			// 构建url
			String url = buildUrl(data);
			// 将URL添加到队列
			SendDataMonitor.addSendUrl(url);
			return true;
		} catch (Throwable e) {
			log.log(Level.WARNING, "发送数据异常", e);
		}
		return false;
	}

	/**
	 * 触发订单退款事件，发送退款数据到服务器
	 * 
	 * @Title: onChargeRefund
	 * 
	 * @param orderId
	 *            退款订单id
	 * @param memberId
	 *            退款会员id
	 * @return 如果发送数据成功，返回true，否则返回false
	 */
	public static boolean onChargeRefund(String orderId, String memberId) {
		try {
			if (isEmpty(orderId) || isEmpty(memberId)) {
				log.log(Level.WARNING, "订单id和会员id不能为空！");
				return false;
			}
			// orderid 和memberid不为空
			Map<String, String> data = new HashMap<>(16);
			data.put("u_id", memberId);
			data.put("oid", orderId);
			data.put("c_time", String.valueOf(System.currentTimeMillis()));
			data.put("ver", VERSION);
			data.put("en", "e_cr");
			data.put("pl", PLATFOR_NAME);
			data.put("sdk", SDK_NAME);
			// 构建url
			String url = buildUrl(data);
			// 将URL添加到队列
			SendDataMonitor.addSendUrl(url);
			return true;
		} catch (Throwable e) {
			log.log(Level.WARNING, "发送数据异常", e);
		}
		return false;
	}

	/**
	 * 根据传入参数构建url
	 * 
	 * @Title: buildUrl
	 * 
	 * @param data
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private static String buildUrl(Map<String, String> data) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		sb.append(ACCESS_URL).append("?");
		for (Map.Entry<String, String> entry : data.entrySet()) {
			if (isNotEmpty(entry.getValue()) && isNotEmpty(entry.getKey())) {
				sb.append(entry.getKey().trim()).append("=").append(URLEncoder.encode(entry.getValue().trim(), "utf-8"))
						.append("&");
			}
		}
		return sb.substring(0, sb.length() - 1);
	}

	/**
	 * 判断字符串是否为空，如果为空返回true，否则返回false
	 * 
	 * @Title: isEmpty
	 * 
	 * @param value
	 * @return
	 */
	private static boolean isEmpty(String value) {

		return value == null || value.trim().isEmpty();
	}

	/**
	 * 判断字符串是否为空，如果不为空，返回true，否则返回false 与isEmpty(String value)相反
	 * 
	 * @Title: isNotEmpty
	 * 
	 * @param value
	 * @return
	 */
	private static boolean isNotEmpty(String value) {
		return !isEmpty(value);
	}
}
