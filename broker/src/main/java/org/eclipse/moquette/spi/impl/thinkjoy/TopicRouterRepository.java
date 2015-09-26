package org.eclipse.moquette.spi.impl.thinkjoy;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.cloudstack.context.CloudContextFactory;
import cn.thinkjoy.im.common.IMConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.List;

/**
 * 创建人：xy
 * 创建时间：15/4/9
 *
 * @version 1.0
 */

public final class TopicRouterRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouterRepository.class);
	private static String NODE_ID = null;
	private static RedisRepository<String, String> redisRepository;
	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository(IMConfig.CACHE_TOPIC_ROUTING_TABLE.get());
			redisRepository.getRedisTemplate().setEnableTransactionSupport(true);
			redisRepository.getRedisTemplate().setValueSerializer(new StringRedisSerializer());

			NODE_ID = CloudContextFactory.getCloudContext().getId();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	public static final void add(final String topic) {
		try {
			final String key = buildTopicCounterKey(topic, NODE_ID);
			redisRepository.incr(key, 1L);
			redisRepository.sAdd(topic, NODE_ID);
			LOGGER.trace("add [topic]:{} to [node]:{}", topic, NODE_ID);
		} catch (Exception e) {
			LOGGER.error("add [topic router] {} fail.", topic);
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	public static final void addBatch(final List<String> topics) {
		try {
			final RedisTemplate<String, String> redisTemplate = redisRepository.getRedisTemplate();
			redisTemplate.setValueSerializer(new StringRedisSerializer());
			redisTemplate.executePipelined(new SessionCallback() {
				@Override
				public Object execute(RedisOperations redisOperations) throws DataAccessException {
					for (String topic : topics) {
						String key = buildTopicCounterKey(topic, NODE_ID);
						redisOperations.multi();
						redisOperations.opsForValue().increment(key, 1L);
						redisOperations.opsForSet().add(topic, NODE_ID);
						redisOperations.exec();
					}
					return null;
				}
			});

//			final RedisTemplate<String, String> redisTemplate = redisRepository.getRedisTemplate();
//			redisTemplate.executePipelined(new RedisCallback() {
//				@Override
//				public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
//					redisConnection
//					StringRedisConnection stringRedisConnection = (StringRedisConnection) redisConnection;
//					for (String topic : topics) {
//						String key = buildTopicCounterKey(topic, NODE_ID);
//						stringRedisConnection.incrBy(key, 1d);
//						stringRedisConnection.sAdd(topic, NODE_ID);
//					}
//					return null;
//				}
//			});

			LOGGER.debug("add [topic]:{} to [node]:{}", topics.toArray(new String[topics.size()]).toString());
		} catch (Exception e) {
			LOGGER.error("add [topic router] {} fail.", topics.toArray(new String[topics.size()]).toString());
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	/**
	 * 清除客户端所的topic信息
	 * 注意： 先要判断topic和nodeid组成的key上的计数器是否大于1，如果大于1，那么可以清除掉该信息，
	 *       否则需将计数器-1即可。
	 *
	 * @param topic
	 */
	public static final void clean(final String topic) {
		try {
			final String key = buildTopicCounterKey(topic, NODE_ID);
			String val = redisRepository.get(key);
			if (null != val) {
				if (Integer.parseInt(val.toString()) > 1) {
					redisRepository.incr(key, -1L);
				} else {
					//clear counter
					redisRepository.del(key);
					//clear data
					if (redisRepository.sIsMember(topic, NODE_ID)) {
						redisRepository.sRem(topic, NODE_ID);
					}
				}
				LOGGER.debug("del [topic]:{} on [node]:{}", topic, NODE_ID);
			}
		} catch (Exception e) {
			LOGGER.error("clean [topic router] {} fail.", topic);
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	public static final void cleanBatch(final String topic) {
		try {
			final String key = buildTopicCounterKey(topic, NODE_ID);
			redisRepository.getRedisTemplate().executePipelined(new RedisCallback() {
				@Override
				public Object doInRedis(RedisConnection connection) throws DataAccessException {
					//redisOperations.boundValueOps(key).get();

//					Object val = redisOperations.opsForValue().get(key);
//					if (null != val) {
//						if (Integer.parseInt(val.toString()) > 1) {
//							redisOperations.opsForValue().increment(key, -1L);
//						} else {
//							//clear counter
//							redisOperations.delete(key);
//							//clear data
//							redisOperations.opsForSet().remove(topic, NODE_ID);
//
//						}
//					}
//					StringRedisConnection stringRedisConn = (StringRedisConnection)connection;
//					String val = stringRedisConn.get(key);
//					if(null != val){
//						if(Integer.parseInt(val) > 1){
//							stringRedisConn.incrBy(key, -1d);
//						}else {
//							stringRedisConn.del(key);
//							stringRedisConn.sRem(topic, NODE_ID);
//						}
//
//					}
					return null;
				}
			});

		} catch (Exception e) {
			LOGGER.error("clean [topic router] {} fail.", topic);
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	private final static String buildTopicCounterKey(String topic, String nodeId) {
		return new StringBuilder("tnc").append(":").append(topic).append(":").append(nodeId).toString();
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
			LOGGER.debug("clean topic [{}] node [{}]", topic, NODE_ID);
		}
	}
}
