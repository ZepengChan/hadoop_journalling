package com.wyu.transformer.service.impl;

import com.wyu.transformer.model.dim.base.*;
import com.wyu.transformer.service.IDimensionConverter;
import com.wyu.util.JdbcManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 公共维度信息数据库操作类
 * 
 * @author ken
 */
public class DimensionConverterImpl implements IDimensionConverter {

	private static final Logger logger = Logger.getLogger(DimensionConverterImpl.class);
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://192.168.123.107:3306/report?useUnicode=true&characterEncoding=UTF8";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "123456";
	static {
		try {
			Class.forName(DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
		@Override
		protected boolean removeEldestEntry(Entry<String, Integer> eldest) {
			return this.size() > 5000;
		}
	};

	@Override
	public int getDimensionIdByValue(BaseDimension dimension) throws IOException {
		String cacheKey = this.buildCacheKey(dimension);
		if (this.cache.containsKey(cacheKey)) {
			return this.cache.get(cacheKey);
		}
		Connection conn = null;
		try {
			conn = this.getConnection();
			/*
			 * 1.查询数据库中是否有对应的值，有则返回 2. 如果第一步中，没有值；先插入我们的dimension数据，再获取id
			 */
			String[] sql;
			if (dimension instanceof DateDimension) {
				sql = buildDateSql();
			} else if (dimension instanceof PlatformDimension) {
				sql = buildPlatformSql();
			} else if (dimension instanceof BrowserDimension) {
				sql = buildBrowserSql();
			} else if (dimension instanceof KpiDimension) {
				sql = this.buildKpiSql();
			} else if (dimension instanceof LocationDimension) {
				sql = this.buildLocationSql();
			}else {
				throw new IOException("不支持此dimensionId的获取：" + dimension.getClass());
			}
			int id ;
			synchronized (this) {
				id = this.executeSql(conn, cacheKey, sql, dimension);
			}
			return id;
		} catch (Exception e) {
			logger.error("操作数据库异常", e);
			throw new IOException(e);
		}
	}

	/**
	 * 获取数据库连接
	 *
	 * @return
	 * @throws SQLException
	 */
	private Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USERNAME, PASSWORD);
	}

	/**
	 * 创建cache key
	 *
	 * @param dimension
	 * @return
	 */
	private String buildCacheKey(BaseDimension dimension) {
		StringBuilder sb = new StringBuilder();
		if (dimension instanceof DateDimension) {
			sb.append("date_dimension");
			DateDimension date = (DateDimension) dimension;
			sb.append(date.getYear()).append(date.getSeason()).append(date.getMonth());
			sb.append(date.getWeek()).append(date.getDay()).append(date.getType());
		} else if (dimension instanceof PlatformDimension) {
			sb.append("platform_dimension");
			PlatformDimension platForm = (PlatformDimension) dimension;
			sb.append(platForm.getPlatformName());
		} else if (dimension instanceof BrowserDimension) {
			sb.append("browser_dimension");
			BrowserDimension browser = (BrowserDimension) dimension;
			sb.append(browser.getBrowserName()).append(browser.getBrowserVersion());
		} else if (dimension instanceof KpiDimension) {
			sb.append("kpi_dimension");
			KpiDimension kpi = (KpiDimension) dimension;
			sb.append(kpi.getKpiName());
		}else if (dimension instanceof LocationDimension) {
			sb.append("location_dimension");
			LocationDimension location = (LocationDimension) dimension;
			sb.append(location.getCountry()).append(location.getProvince()).append(location.getCity());
		}

		if (sb.length() == 0) {
			throw new RuntimeException("无法创建指定dimension的cacheKey" + dimension.getClass());
		}
		return sb.toString();
	}

	private void setArgs(PreparedStatement psmt, BaseDimension dimension) throws SQLException {
		int i = 0;

		if (dimension instanceof DateDimension) {
			DateDimension date = (DateDimension) dimension;
			psmt.setInt(++i, date.getYear());
			psmt.setInt(++i, date.getSeason());
			psmt.setInt(++i, date.getMonth());
			psmt.setInt(++i, date.getWeek());
			psmt.setInt(++i, date.getDay());
			psmt.setString(++i, date.getType());
			psmt.setDate(++i, new Date(date.getCalendar().getTime()));
		} else if (dimension instanceof PlatformDimension) {
			PlatformDimension platForm = (PlatformDimension) dimension;
			psmt.setString(++i, platForm.getPlatformName());
		} else if (dimension instanceof BrowserDimension) {
			BrowserDimension browser = (BrowserDimension) dimension;
			psmt.setString(++i, browser.getBrowserName());
			psmt.setString(++i, browser.getBrowserVersion());
		}else if (dimension instanceof KpiDimension) {
            KpiDimension kpi = (KpiDimension) dimension;
            psmt.setString(++i, kpi.getKpiName());
        }else if (dimension instanceof LocationDimension) {
            LocationDimension location = (LocationDimension) dimension;
            psmt.setString(++i, location.getCountry());
            psmt.setString(++i, location.getProvince());
            psmt.setString(++i, location.getCity());
        }
	}

	/**
	 * 创建date dimension相关sql
	 *
	 * @return
	 */
	private String[] buildDateSql() {
		String querySql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?";
		String insertSql = "INSERT INTO `dimension_date`(`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 创建platform dimension相关sql
	 *
	 * @return
	 */
	private String[] buildPlatformSql() {
		String querySql = "SELECT `id` FROM `dimension_platform` WHERE `platform_name` = ?";
		String insertSql = "INSERT INTO `dimension_platform`(`platform_name`) VALUES(?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 创建borwser dimension相关sql
	 *
	 * @return
	 */
	private String[] buildBrowserSql() {
		String querySql = "SELECT `id` FROM `dimension_browser` WHERE `browser_name` = ? AND `browser_version` = ?";
		String insertSql = "INSERT INTO `dimension_browser`(`browser_name`, `browser_version`) VALUES(?, ?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 创建kpi dimension相关sql
	 * 
	 * @return
	 */
	private String[] buildKpiSql() {
		String querySql = "SELECT `id` FROM `dimension_kpi` WHERE `kpi_name` = ?";
		String insertSql = "INSERT INTO `dimension_kpi`(`kpi_name`) VALUES(?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 创建location dimension相关sql
	 *
	 * @return
	 */
	private String[] buildLocationSql() {
		String querySql = "SELECT `id` FROM `dimension_location` WHERE `country` = ? AND `province` = ? AND `city` = ?";
		String insertSql = "INSERT INTO `dimension_location`(`country`,`province`,`city`) VALUES(?,?,?)";
		return new String[] { querySql, insertSql };
	}

	/**
	 * 具体执行sql的方法
	 *
	 * @param conn
	 * @param catchKey
	 * @param sqls
	 * @param dimension
	 * @return
	 */
	private int executeSql(Connection conn, String catchKey, String[] sqls, BaseDimension dimension)
			throws SQLException {

		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {
			psmt = conn.prepareStatement(sqls[0]);
			this.setArgs(psmt, dimension);
			rs = psmt.executeQuery();
			if (rs.next()) {
				/* 返回第一列值 ID */
				return rs.getInt(1);
			}
			/* 代码可以执行到这，表示数据库无dimension数据,执行插入操作 */
			psmt = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
			this.setArgs(psmt, dimension);
			psmt.execute();
			System.out.println(psmt.toString());
			rs = psmt.getGeneratedKeys();
			if (rs.next()) {
				return rs.getInt(1);
			}
		} finally {
			JdbcManager.close(conn,psmt,rs);
		}
		throw new RuntimeException("从数据库获取id失败！");
	}
}
