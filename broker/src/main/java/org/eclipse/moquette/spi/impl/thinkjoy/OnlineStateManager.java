package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.Objects;
import java.util.Set;

import cn.thinkjoy.im.common.ClientIds;
import com.google.common.collect.Sets;
import org.eclipse.moquette.commons.Constants;
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

	//empty constructor
	private OnlineStateManager() {
	}

	/**
	 * 将用户连接的用户放入缓存中, userID为key,clientID为value
	 * userID = accountArea+account
	 * e.g : accountArea = zhiliao
	 * account = testuser
	 * userID = zhiliaotestuser
	 *
	 * @param clientID
	 */
	public static final void put(String clientID) {
		Jedis jedis = null;
		try {
			String userID = buildUserID(clientID);
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
	 * 构建userID
	 * userID = accountArea+account
	 * e.g : accountArea = zhiliao
	 * account = testuser
	 * userID = zhiliaotestuser
	 *
	 * @param clientID
	 * @return
	 */
	private static String buildUserID(String clientID) {
		String accountArea = ClientIds.getAccountArea(clientID);
		String account = ClientIds.getAccount(clientID);
		return accountArea.concat(account);
	}

	/**
	 * 将断开连接的用户从缓存中清除, userID为key,clientID为value
	 *
	 * @param clientID
	 */
	public static final void remove(String clientID) {
		Jedis jedis = null;
		try {
			String userID = buildUserID(clientID);
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

	public static final Set<String> get(String clientID) {
		Jedis jedis = null;
		try {
			String userID = buildUserID(clientID);
			jedis = RedisPool.getPool().getResource();
			Set<String> members = jedis.smembers(userID);
			if (members.size() > 0) {
				return members;
			} else {
				return Sets.newHashSet();
			}
		} catch (Exception e) {
			LOG.error(String.format("query [isOnline] %s fail.", clientID));
			throw new MQTTException("query [isOnline] fail:" + clientID);
		} finally {
			RedisPool.getPool().returnResource(jedis);
		}
	}

	public static final boolean isAllowMutiClient(String clientID) {
		Jedis jedis = null;
		try {
			String accountArea = ClientIds.getAccountArea(clientID);
			jedis = RedisPool.getPool().getResource();
			Set<String> smembers = jedis.smembers(Constants.KEY_MUTI_CLIENT_ALLOWABLE);
			for (String smember : smembers) {
				if (Objects.equals(smember, accountArea)) {
					return true;
				}
			}
		} catch (Exception e) {
			LOG.error(String.format("query [isOnline] %s fail.", clientID));
			throw new MQTTException("query [isOnline] fail:" + clientID);
		} finally {
			RedisPool.getPool().returnResource(jedis);
		}
		return false;
	}
}
