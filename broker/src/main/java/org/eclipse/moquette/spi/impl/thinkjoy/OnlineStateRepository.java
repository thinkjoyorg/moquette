package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.Set;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.im.common.ClientIds;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.eclipse.moquette.commons.Constants;
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

public final class OnlineStateRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(OnlineStateRepository.class);

	private static final String STR = "_";

	private static RedisRepository<String, String> redisRepository;

	static {
		try {
			redisRepository = RedisRepositoryFactory.getRepository("im-connector", "common", "redis");
			redisRepository.getRedisTemplate().setEnableTransactionSupport(true);
			redisRepository.getRedisTemplate().setValueSerializer(new StringRedisSerializer());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
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
			LOGGER.info("[User]:{} is online on [clientID]:{}", userID, clientID);
		} catch (Exception e) {
			LOGGER.error(String.format("put [userState] %s fail.", clientID));
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
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
		Preconditions.checkNotNull(clientID, "clientID must not null");

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
			if (redisRepository.sIsMember(userID, clientID)) {
				redisRepository.sRem(userID, clientID);
			}
			LOGGER.info("[User]:{} is offline on [clientID]:{}", userID, clientID);
		} catch (Exception e) {
			LOGGER.error(String.format("remove [userState] %s fail.", clientID));
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
		}
	}

	public static final Set<String> get(String clientID) {
		try {
			String userID = buildUserID(clientID);
			Set<String> result = redisRepository.sMembers(userID);
			return result;
		} catch (Exception e) {
			LOGGER.error(String.format("query [isOnline] %s fail.", clientID));
			LOGGER.error(e.getMessage(), e);
			throw new RedisSystemException(e.getMessage(), e);
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
			LOGGER.error(e.getMessage(), e);
			//异常情况，默认kick
			return Constants.KICK;
		}
	}
}
