package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.cloudstack.context.CloudContextFactory;
import com.google.common.base.Strings;
import org.eclipse.moquette.proto.MQTTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			System.exit(-1);
		}
	}

	public static final void addRoute(String topic) {
		try {
			String nodeId = CloudContextFactory.getCloudContext().getId();

			redisRepository.getRedisTemplate().multi();

			redisRepository.incr(buildTopicCounterKey(topic, nodeId), 1);
			redisRepository.sAdd(topic, nodeId);

			redisRepository.getRedisTemplate().exec();
			LOGGER.info("add [topic]:{} to [node]:{}", topic, nodeId);
		} catch (Exception e) {
			redisRepository.getRedisTemplate().discard();
			LOGGER.error(String.format("add [topic router] %s fail.", topic));
			LOGGER.error(e.getMessage());
			throw new MQTTException("add [topic router] fail:" + topic);
		}
	}

	/**
	 * 当客户端退订时，清除客户端所的topic信息
	 * 注意： 退订时，先要判断topic和nodeid组成的key上的计数器是否为0，如果为0，那么可以清除掉该信息，
	 *       如果不为0，不能清除该信息，只需将计数器-1即可。
	 *
	 * @param topic
	 */
	public static final void cleanRouteTopicNode(String topic) {
		try {
			String nodeId = CloudContextFactory.getCloudContext().getId();

			redisRepository.getRedisTemplate().multi();
			String key = buildTopicCounterKey(topic, nodeId);
			String val = redisRepository.get(key);
			if (!Strings.isNullOrEmpty(val) && Integer.parseInt(val) != 0) {
				redisRepository.incr(key, -1);
				LOGGER.info("decr [key]:{} , current [val]:{}", key, val);
			} else {
				redisRepository.sRem(topic, nodeId);
				LOGGER.info("del [topic]:{} on [node]:{}", topic, nodeId);
			}
			redisRepository.getRedisTemplate().exec();

		} catch (Exception e) {
			redisRepository.getRedisTemplate().discard();
			LOGGER.error(String.format("del [topic] %s fail.", topic));
			LOGGER.error(e.getMessage());
			throw new MQTTException("del [topic] fail:" + topic);
		}

	}

	private final static String buildTopicCounterKey(String topic, String nodeId) {
		return new StringBuilder("topicNodeCounter").append(":").append(topic).append(nodeId).toString();
	}
}
