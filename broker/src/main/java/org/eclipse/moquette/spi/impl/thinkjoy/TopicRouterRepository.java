package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.cloudstack.context.CloudContextFactory;
import org.eclipse.moquette.proto.MQTTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
			LOGGER.error(e.getMessage());
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
			LOGGER.error(e.getMessage());
			throw new MQTTException("add [topic router] fail:" + topic);
		}
//		try {
//			final String nodeId = CloudContextFactory.getCloudContext().getId();
//			final String key = buildTopicCounterKey(topic, nodeId);
//
//			redisRepository.getRedisTemplate().execute(new SessionCallback<List<Object>>() {
//				@Override
//				public List<Object> execute(RedisOperations redisOperations) throws DataAccessException {
//					try{
//						redisOperations.multi();
//						redisOperations.opsForValue().increment(key, 1L);
//						redisOperations.opsForSet().add(topic, nodeId);
//						return redisOperations.exec();
//					}catch (Exception e){
//						redisOperations.discard();
//						LOGGER.error(e.getMessage());
//						throw new MQTTException("add [topic router] fail:" + topic);
//					}
//				}
//			});

//			LOGGER.info("add [topic]:{} to [node]:{}", topic, nodeId);
//		} catch (Exception e) {
//			LOGGER.error(String.format("add [topic router] %s fail.", topic));
//			LOGGER.error(e.getMessage());
//			throw new MQTTException("add [topic router] fail:" + topic);
//		}
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

//			redisRepository.getRedisTemplate().execute(new SessionCallback<List<Object>>() {
//				@Override
//				public List<Object> execute(RedisOperations redisOperations) throws DataAccessException {
//					try {
//
//						Object val = redisOperations.opsForValue().get(key);
//						redisOperations.multi();
//
//						if (null != val && Integer.parseInt(val.toString()) != 0) {
//							redisOperations.opsForValue().increment(key, -1L);
//							LOGGER.debug("decr counter [key]:{} , current [val]:{}", key, val);
//						} else {
//							if (redisOperations.opsForSet().isMember(topic, nodeId)) {
//								redisOperations.opsForSet().remove(topic, nodeId);
//								LOGGER.debug("del [topic]:{} on [node]:{}", topic, nodeId);
//							}
//						}
//						return redisOperations.exec();
//					} catch (Exception e) {
//						redisOperations.discard();
//						LOGGER.error(e.getMessage());
//						throw new MQTTException("del [topic] fail:" + topic);
//					}
//				}
//			});
			String val = redisRepository.get(key);
			if (null != val && Integer.parseInt(val.toString()) != 0) {
				redisRepository.incr(key, -1L);
				LOGGER.debug("decr counter [key]:{} , current [val]:{}", key, val);
			} else {
				if (redisRepository.sMembers(topic) != null && redisRepository.sMembers(topic).size() > 0) {
					redisRepository.sRem(topic, nodeId);
					LOGGER.debug("del [topic]:{} on [node]:{}", topic, nodeId);
				}
			}
//			if (redisRepository.sMembers(topic).size() > 0) {
//				redisRepository.sRem(topic, nodeId);
//			}
			LOGGER.debug("del [topic]:{} on [node]:{}", topic, nodeId);
		} catch (Exception e) {
			LOGGER.error(String.format("clean [topic router] %s fail.", topic));
			LOGGER.error(e.getMessage());
			throw new MQTTException("clean [topic router] fail:" + topic);
		}
	}

	private final static String buildTopicCounterKey(String topic, String nodeId) {
		return new StringBuilder("topicNodeCounter").append(":").append(topic).append(nodeId).toString();
	}
}
