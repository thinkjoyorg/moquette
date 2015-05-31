package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.concurrent.TimeUnit;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.im.common.IMConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * 接入方在连接的时候会进行token的认证，如果通过，则可以进行连接。
 * 并且将token放入redis中，http服务仅仅认证redis中的token。
 * 创建人：xy
 * 创建时间：15/4/17
 *
 * @version 1.0
 */

public final class TokenRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(TokenRepository.class);

	private static RedisRepository<String, String> redisRepository;

	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository(IMConfig.CACHE_USER_TOKEN.get());
			redisRepository.getRedisTemplate().setValueSerializer(new StringRedisSerializer());

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	/**
	 * token的缓存。
	 * 先从缓存中查询，如果没有查到，再调用rpc接口进行认证，通过后再放入缓存。
	 *
	 * @param token
	 * @return
	 */
	public static boolean authToken(String token) {
		try {
			String result = redisRepository.get(buildTokenKey(token));
			return result == null ? false : true;
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
			return false;
		}
	}

	/**
	 * 缓存 远程调用取得的token
	 *
	 * @param token
	 * @param ttl
	 */
	public static void set(String token, long ttl) {
		try {
			redisRepository.set(buildTokenKey(token), token, ttl, TimeUnit.SECONDS);
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
	}

	private static final String buildTokenKey(String token) {
		String key = new StringBuilder("token").append(":").append(token).toString();
		return key;
	}
}
