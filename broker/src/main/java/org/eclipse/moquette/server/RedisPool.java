package org.eclipse.moquette.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 创建人：xy
 * 创建时间：15/3/10
 *
 * @version 1.0
 */

public class RedisPool {
	private static final Logger log = LoggerFactory.getLogger(RedisPool.class);
	private static JedisPool pool;

	static {
		InputStream inputStream = null;
		String configPath = System.getProperty("moquette.path", null);
		Properties prop = new Properties();
		try {
			inputStream = new FileInputStream(new File(configPath, "config/redis.properties"));
			prop.load(inputStream);
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			if (null != inputStream) {
				try {
					inputStream.close();
				} catch (IOException e) {
					//ignore
				}
			}
		}
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
