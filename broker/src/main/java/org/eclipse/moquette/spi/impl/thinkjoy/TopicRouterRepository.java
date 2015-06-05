package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.cloudstack.context.CloudContextFactory;
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

public final class TopicRouterRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouterRepository.class);
	private static final String NODE_ID = CloudContextFactory.getCloudContext().getId();
	private static RedisRepository<String, String> redisRepository;
	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository(IMConfig.CACHE_TOPIC_ROUTING_TABLE.get());
			redisRepository.getRedisTemplate().setEnableTransactionSupport(true);
			redisRepository.getRedisTemplate().setValueSerializer(new StringRedisSerializer());

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	public static final void add(final String topic) {
		try {
//			final String key = buildTopicCounterKey(topic, NODE_ID);
//			redisRepository.incr(key, 1L);
			redisRepository.sAdd(topic, NODE_ID);
			LOGGER.trace("add [topic]:{} to [node]:{}", topic, NODE_ID);
		} catch (Exception e) {
			LOGGER.error("add [topic router] {} fail.", topic);
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	/**
	 * 清除客户端所的topic信息
	 * 注意： 先要判断topic和nodeid组成的key上的计数器是否为0，如果为0，那么可以清除掉该信息，
	 *       如果不为0，不能清除该信息，只需将计数器-1即可。
	 *
	 * @param topic
	 */
	@Deprecated
	public static final void clean(final String topic) {
		try {
			final String key = buildTopicCounterKey(topic, NODE_ID);
			String val = redisRepository.get(key);
			if (null != val && Integer.parseInt(val.toString()) != 0) {
				redisRepository.incr(key, -1L);
			} else {
				if (redisRepository.sMembers(topic) != null && redisRepository.sMembers(topic).size() > 0) {
					redisRepository.sRem(topic, NODE_ID);
				}
			}
			LOGGER.trace("del [topic]:{} on [node]:{}", topic, NODE_ID);
		} catch (Exception e) {
			LOGGER.error("clean [topic router] {} fail.", topic);
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	@Deprecated
	private final static String buildTopicCounterKey(String topic, String nodeId) {
		return new StringBuilder("topicNodeCounter").append(":").append(topic).append(nodeId).toString();
	}

	/**
	 * remove the dead topic node record
	 *
	 * @param topic
	 */
	public static final void remove(final String topic) {
		boolean result = redisRepository.sIsMember(topic, NODE_ID);
		if (!result) {
			redisRepository.sRem(topic, NODE_ID);
		}
		LOGGER.debug("clean topic [{}] node [{}]", topic, NODE_ID);
	}
}
