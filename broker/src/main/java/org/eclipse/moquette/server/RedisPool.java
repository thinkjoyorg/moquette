package org.eclipse.moquette.server;

import java.io.File;
import java.util.Properties;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 创建人：xy
 * 创建时间：15/3/10
 *
 * @version 1.0
 */

public class RedisPool {
	private static JedisPool pool;

	//empty constructor
	private RedisPool() {
	}

	static {
		String configPath = System.getProperty("moquette.path", null);
		Properties prop = PropertyParser.getProperties(new File(configPath, "config/redis.properties"));
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(500);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000 * 100);
		config.setTestOnBorrow(true);

		pool = new JedisPool(prop.getProperty("host"), Integer.parseInt(prop.getProperty("port")));
	}

	public static JedisPool getPool() {
		return pool;
	}
}
