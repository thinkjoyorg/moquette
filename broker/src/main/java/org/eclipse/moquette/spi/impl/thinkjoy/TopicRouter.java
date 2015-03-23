package org.eclipse.moquette.spi.impl.thinkjoy;

import org.eclipse.moquette.proto.MQTTException;
import org.eclipse.moquette.server.RedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * topic到broker的路由。
 * 存储在redis中，以topic为key，以broker的uri为值。
 * <p/>
 * 创建人：xy
 * 创建时间：15/3/20
 *
 * @version 1.0
 */

public final class TopicRouter {
	private static final Logger LOG = LoggerFactory.getLogger(TopicRouter.class);

	//add topic ---> Node router to redis
	public static final void addRoute(String topic, String nodeUri) {
		Jedis jedis = null;
		try {
			jedis = RedisPool.getPool().getResource();
			jedis.sadd(topic, nodeUri);
		} catch (Exception e) {
			LOG.error(String.format("add [topic] %s:%s fail.", topic, nodeUri));
			throw new MQTTException("add [topic] fail:" + topic);
		} finally {
			RedisPool.getPool().returnResource(jedis);
		}
	}

	//remove topic ---> node route from reids.
	public static final void cleanRouteByTopic(String topic) {
		Jedis jedis = null;
		try {
			jedis = RedisPool.getPool().getResource();
			jedis.del(topic);
		} catch (Exception e) {
			LOG.error(String.format("del [topic] %s fail.", topic));
			throw new MQTTException("del [topic] fail:" + topic);
		} finally {
			RedisPool.getPool().returnResource(jedis);
		}
	}

}
