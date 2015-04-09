package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.cloudstack.context.CloudContextFactory;
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

	private final static RedisRepository<String, Object> redisRepository;

	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository("im-service", "common", "topicRouterRedis");
		} catch (Exception e) {
			LOGGER.error("get topic router redis fail...");
			throw new RuntimeException("get topic router redis fail...");
		}
	}

	//add topic ---> Node router to redis
	public static final void addRoute(String topic) {
		try {
			String nodeId = CloudContextFactory.getCloudContext().getId();
			redisRepository.sAdd(topic, nodeId);
		} catch (Exception e) {
			LOGGER.error(String.format("add [topic] %s:%s fail.", topic));
			throw new MQTTException("add [topic] fail:" + topic);
		}
	}

	//remove topic ---> node route from reids.
	public static final void cleanRouteByTopic(String topic) {
		try {
			redisRepository.del(topic);
		} catch (Exception e) {
			LOGGER.error(String.format("del [topic] %s fail.", topic));
			throw new MQTTException("del [topic] fail:" + topic);
		}
	}
}
