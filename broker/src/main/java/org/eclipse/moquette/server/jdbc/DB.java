package org.eclipse.moquette.server.jdbc;

import java.sql.*;
import java.util.Properties;

import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.spi.impl.thinkjoy.AccountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/3/26
 *
 * @version 1.0
 */

public final class DB {
	private static final Logger LOG = LoggerFactory.getLogger(DB.class);

	private String url;
	private String username;
	private String password;
	private String driver;

	public DB(Properties properties) {
		this.driver = properties.getProperty("driver");
		this.url = properties.getProperty("url");
		this.username = properties.getProperty("username");
		this.password = properties.getProperty("password");
	}

	private Connection getConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			LOG.error("数据库连接出错");
		}
		return conn;
	}

	public void initAreaToRedis() {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			String sql = "select account,password from im_acc_area aa where aa.status != -1";
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				String account = rs.getString("account");
				String password = rs.getString("password");
				AccountRepository.set(Constants.KEY_AREA_ACCOUNT, account, password);
			}
		} catch (SQLException e) {
			LOG.error("initAreaToRedis fail...");
			LOG.error(e.getMessage());
		} finally {
			release(connection, ps, rs);
		}
	}

	public void initMutiClientAllowableToRedis() {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			String sql = "select a.account,r.kickOrPrevent from `im_acc_area` a inner join `im_acc_area_rule` r on a.`id` = r.`areaAccountId` where a.`status` != -1";
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				String account = rs.getString("account");
				int kickOrPrevent = rs.getInt("kickOrPrevent");
				AccountRepository.set(Constants.KEY_MUTI_CLIENT_ALLOWABLE, account, String.valueOf(kickOrPrevent));
			}
		} catch (SQLException e) {
			LOG.error("initMutiClientAllowableToRedis fail...");
			LOG.error(e.getMessage());
		} finally {
			release(connection, ps, rs);
		}
	}

	private void release(Connection c, PreparedStatement ps, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				//ignore
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				//ignore
			}
		}
		if (c != null) {
			try {
				c.close();
			} catch (SQLException e) {
				//ignore
			}
		}

	}

}
