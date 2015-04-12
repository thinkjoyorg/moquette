package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/4/9
 *
 * @version 1.0
 */

public final class AccountRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouterRepository.class);

	private final static RedisRepository<String, Object> redisRepository;

	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository("im-connector", "common", "redis");
		} catch (Exception e) {
			LOGGER.error("get area account redis fail...");
			throw new RuntimeException("get area account redis fail...");
		}
	}

	public static final void set(String key, String hKey, String value) {
		redisRepository.hSet(key, hKey, value);
	}

	public static final Object get(String key, String hKey) {
		Object o = redisRepository.hGet(key, hKey);
		return o;
	}
}
