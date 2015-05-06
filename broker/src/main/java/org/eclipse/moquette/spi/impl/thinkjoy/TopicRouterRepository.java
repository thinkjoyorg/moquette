package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.cloudstack.context.CloudContextFactory;
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

public final class TopicRouterRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouterRepository.class);

	private static RedisRepository<String, String> redisRepository;

	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository("im-connector", "common", "redis");
			redisRepository.getRedisTemplate().setEnableTransactionSupport(true);
			redisRepository.getRedisTemplate().setValueSerializer(new StringRedisSerializer());

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	public static final void addRoute(final String topic) {
		try {
			final String nodeId = CloudContextFactory.getCloudContext().getId();
			final String key = buildTopicCounterKey(topic, nodeId);
			redisRepository.incr(key, 1L);
			redisRepository.sAdd(topic, nodeId);
			LOGGER.info("add [topic]:{} to [node]:{}", topic, nodeId);
		} catch (Exception e) {
			LOGGER.error(String.format("add [topic router] %s fail.", topic));
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	/**
	 * 当客户端退订时，清除客户端所的topic信息
	 * 注意： 退订时，先要判断topic和nodeid组成的key上的计数器是否为0，如果为0，那么可以清除掉该信息，
	 *       如果不为0，不能清除该信息，只需将计数器-1即可。
	 *
	 * @param topic
	 */
	public static final void cleanRouteTopicNode(final String topic) {
		try {
			final String nodeId = CloudContextFactory.getCloudContext().getId();
			final String key = buildTopicCounterKey(topic, nodeId);
			String val = redisRepository.get(key);
			if (null != val && Integer.parseInt(val.toString()) != 0) {
				redisRepository.incr(key, -1L);
			} else {
				if (redisRepository.sMembers(topic) != null && redisRepository.sMembers(topic).size() > 0) {
					redisRepository.sRem(topic, nodeId);
				}
			}
			LOGGER.debug("del [topic]:{} on [node]:{}", topic, nodeId);
		} catch (Exception e) {
			LOGGER.error(String.format("clean [topic router] %s fail.", topic));
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	private final static String buildTopicCounterKey(String topic, String nodeId) {
		return new StringBuilder("topicNodeCounter").append(":").append(topic).append(nodeId).toString();
	}
}
