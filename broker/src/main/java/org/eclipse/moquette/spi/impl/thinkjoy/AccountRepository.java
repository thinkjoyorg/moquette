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
	private static final Logger LOGGER = LoggerFactory.getLogger(AccountRepository.class);

	private static RedisRepository<String, String> redisRepository;

	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository("im-connector", "common", "redis");
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	public static final void set(String key, String hKey, String value) {
		redisRepository.hSet(key, hKey, value);
	}

	public static final String get(String key, String hKey) {
		String res = redisRepository.hGet(key, hKey);
		return res;
	}
}
