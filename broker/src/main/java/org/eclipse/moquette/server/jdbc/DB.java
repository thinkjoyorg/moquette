package org.eclipse.moquette.server.jdbc;

import java.sql.*;
import java.util.Properties;

import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.server.RedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

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
		Jedis resource = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			String sql = "select account,password from im_area_account aa where aa.status != -1";
			ps = connection.prepareStatement(sql);
			resource = RedisPool.getPool().getResource();
			rs = ps.executeQuery();
			while (rs.next()) {
				String account = rs.getString("account");
				String password = rs.getString("password");
				resource.hset(Constants.KEY_AREA_ACCOUNT, account, password);
			}
		} catch (SQLException e) {
			LOG.error("initAreaToRedis fail...");
		} finally {
			release(connection, ps, rs, resource);
		}
	}

	public void initMutiClientAllowableToRedis() {
		Connection connection = null;
		PreparedStatement ps = null;
		Jedis resource = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			String sql = "select a.account,r.kickOrPrevent from `im_area_account` a inner join `im_area_acc_rule` r on a.`id` = r.`areaAccountId` where a.`status` != -1";
			ps = connection.prepareStatement(sql);
			resource = RedisPool.getPool().getResource();
			rs = ps.executeQuery();
			while (rs.next()) {
				String account = rs.getString("account");
				int kickOrPrevent = rs.getInt("kickOrPrevent");
				resource.hset(Constants.KEY_MUTI_CLIENT_ALLOWABLE, account, String.valueOf(kickOrPrevent));
			}
		} catch (SQLException e) {
			LOG.error("initMutiClientAllowableToRedis fail...");
		} finally {
			release(connection, ps, rs, resource);
		}
	}

	private void release(Connection c, PreparedStatement ps, ResultSet rs, Jedis resource) {
		if (rs != null)
			try {
				rs.close();
			} catch (SQLException e) {
				//ignore
			}
		if (ps != null)
			try {
				ps.close();
			} catch (SQLException e) {
				//ignore
			}
		if (c != null)
			try {
				c.close();
			} catch (SQLException e) {
				//ignore
			}
		if (resource != null)
			RedisPool.getPool().returnResource(resource);
	}

}
