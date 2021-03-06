package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.im.common.IMConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.serializer.StringRedisSerializer;

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
			redisRepository = RedisRepositoryFactory.getRepository(IMConfig.CACHE_ACCOUNT_AREA.get());
			redisRepository.getRedisTemplate().setEnableTransactionSupport(true);
			redisRepository.getRedisTemplate().setValueSerializer(new StringRedisSerializer());
			redisRepository.getRedisTemplate().setHashValueSerializer(new StringRedisSerializer());

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	public static final void set(String key, String hKey, String value) {
		try {
			redisRepository.hSet(key, hKey, value);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	public static final String get(String key, String hKey) {
		try {
			String res = redisRepository.hGet(key, hKey);
			return res;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}
}
