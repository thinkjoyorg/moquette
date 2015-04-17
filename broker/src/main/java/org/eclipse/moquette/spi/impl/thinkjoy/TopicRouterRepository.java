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
			redisRepository.sAdd(topic, nodeId);
		} catch (Exception e) {
			LOGGER.error(String.format("add [topic router] %s fail.", topic));
			throw new MQTTException("add [topic router] fail:" + topic);
		}
	}

	/**
	 * 当客户端断开连接时，清除该节点上所有客户端所订阅的topic
	 *
	 * @param topic
	 */
	public static final void cleanRouteByTopic(String topic) {
		try {
			String nodeId = CloudContextFactory.getCloudContext().getId();
			redisRepository.sRem(topic, nodeId);
		} catch (Exception e) {
			LOGGER.error(String.format("del [topic] %s fail.", topic));
			throw new MQTTException("del [topic] fail:" + topic);
		}
	}

	/**
	 * 当客户端退订时，清除客户端所退订的topic信息
	 *
	 * @param topic
	 */
	public static final void cleanRouteTopicNode(String topic) {
		try {
			String nodeId = CloudContextFactory.getCloudContext().getId();
			redisRepository.sRem(topic, nodeId);
		} catch (Exception e) {
			LOGGER.error(String.format("del [topic] %s fail.", topic));
			throw new MQTTException("del [topic] fail:" + topic);
		}

	}
}
