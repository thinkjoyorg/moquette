package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.Set;

import cn.thinkjoy.im.common.ClientIds;
import org.eclipse.moquette.proto.MQTTException;
import org.eclipse.moquette.server.RedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * 用户的在线状态维护。
 * 创建人：xy
 * 创建时间：15/3/23
 *
 * @version 1.0
 */

public final class OnlineStateManager {
	private static final Logger LOG = LoggerFactory.getLogger(OnlineStateManager.class);

	/**
	 * 将用户连接的用户放入缓存中, userID为key,clientID为value
	 * userID = accountArea+account
	 * e.g : accountArea = zhiliao
	 * account = testuser
	 * userID = zhiliaotestuser
	 *
	 * @param clientID
	 */
	public static void put(String clientID) {
		Jedis jedis = null;
		try {
			String accountArea = ClientIds.getAccountArea(clientID);
			String account = ClientIds.getAccount(clientID);
			String userID = accountArea.concat(account);
			jedis = RedisPool.getPool().getResource();
			jedis.sadd(userID, clientID);
		} catch (Exception e) {
			LOG.error(String.format("put [userState] %s fail.", clientID));
			throw new MQTTException("put [userState] fail:" + clientID);
		} finally {
			RedisPool.getPool().returnResource(jedis);
		}
	}

	/**
	 * 将断开连接的用户从缓存中清除, userID为key,clientID为value
	 * * userID = accountArea+account
	 * e.g : accountArea = zhiliao
	 * account = testuser
	 * userID = zhiliaotestuser
	 *
	 * @param clientID
	 */
	public static void remove(String clientID) {
		Jedis jedis = null;
		try {
			String accountArea = ClientIds.getAccountArea(clientID);
			String account = ClientIds.getAccount(clientID);
			String userID = accountArea.concat(account);
			jedis = RedisPool.getPool().getResource();
			Set<String> members = jedis.smembers(userID);
			for (String member : members) {
				if (clientID.equals(member)) {
					jedis.srem(userID, member);
				}
			}
		} catch (Exception e) {
			LOG.error(String.format("remove [userState] %s fail.", clientID));
			throw new MQTTException("remove [userState] fail:" + clientID);
		} finally {
			RedisPool.getPool().returnResource(jedis);
		}
	}
}
