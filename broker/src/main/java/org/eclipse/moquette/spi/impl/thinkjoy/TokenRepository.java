package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.concurrent.TimeUnit;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
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
			//token放在业务节点中
			redisRepository = RedisRepositoryFactory.getRepository("im-service", "common", "redis");
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
		String result = redisRepository.get(buildTokenKey(token));
		return result == null ? false : true;
	}

	/**
	 * 缓存 远程调用取得的token
	 *
	 * @param token
	 * @param ttl
	 */
	public static void set(String token, long ttl) {
		redisRepository.set(buildTokenKey(token), token, ttl, TimeUnit.SECONDS);
	}

	private static final String buildTokenKey(String token) {
		String key = new StringBuilder("token").append(":").append(token).toString();
		return key;
	}
}
