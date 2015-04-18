package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.Set;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.im.common.ClientIds;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.proto.MQTTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/4/9
 *
 * @version 1.0
 */

public final class OnlineStateRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(OnlineStateRepository.class);

	private static final String STR = "_";

	private static RedisRepository<String, String> redisRepository;

	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository("im-connector", "common", "redis");
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			System.exit(-1);
		}
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
		try {
			String userID = buildUserID(clientID);
			redisRepository.sAdd(userID, clientID);
			LOGGER.info("[User]: is online on [clientID]:{}", userID, clientID);
		} catch (Exception e) {
			LOGGER.error(String.format("put [userState] %s fail.", clientID));
			LOGGER.error(e.getMessage());
			throw new MQTTException("put [userState] fail:" + clientID);
		}
	}

	/**
	 * 构建userID
	 * userID = accountArea+account
	 * e.g : accountArea = zhiliao
	 * account = testuser
	 * userID = zhiliao_testuser
	 *
	 * @param clientID
	 * @return
	 */
	private static String buildUserID(String clientID) throws Exception {
		String accountArea = ClientIds.getAccountArea(clientID);
		String account = ClientIds.getAccount(clientID);
		StringBuilder builder = new StringBuilder(accountArea);
		builder.append(STR);
		builder.append(account);
		return builder.toString();
	}

	/**
	 * 将断开连接的用户从缓存中清除, userID为key,clientID为value
	 *
	 * @param clientID
	 */
	public static final void remove(String clientID) {
		try {
			String userID = buildUserID(clientID);
			Set<String> members = redisRepository.sMembers(userID);
			if (members.size() > 0) {
				for (String member : members) {
					if (clientID.equals(member)) {
						redisRepository.sRem(userID, member);
					}
				}
			}
			LOGGER.info("[User]: is offline on [clientID]:{}", userID, clientID);
		} catch (Exception e) {
			LOGGER.error(String.format("remove [userState] %s fail.", clientID));
			LOGGER.error(e.getMessage());
			throw new MQTTException("remove [userState] fail:" + clientID);
		}
	}

	public static final Set<String> get(String clientID) {
		try {
			String userID = buildUserID(clientID);
			Set<String> members = redisRepository.sMembers(userID);
			if (members.size() > 0) {
				return members;
			} else {
				return Sets.newHashSet();
			}
		} catch (Exception e) {
			LOGGER.error(String.format("query [isOnline] %s fail.", clientID));
			LOGGER.error(e.getMessage());
			throw new MQTTException("query [isOnline] fail:" + clientID);
		}
	}

	//查询该clientID所属域账号下的多终端登录策略,kick or prevent
	public static final int getMutiClientAllowable(String clientID) {
		try {
			String accountArea = ClientIds.getAccountArea(clientID);
			Optional<String> kickOrPrevent = Optional.of(AccountRepository.get(Constants.KEY_MUTI_CLIENT_ALLOWABLE, accountArea));
			return Integer.parseInt(kickOrPrevent.get());
		} catch (Exception e) {
			LOGGER.error(String.format("query [mutiClientAllowable] %s fail.", clientID));
			LOGGER.error(e.getMessage());
			throw new MQTTException("query [mutiClientAllowable] fail:" + clientID);
		}
	}
}
