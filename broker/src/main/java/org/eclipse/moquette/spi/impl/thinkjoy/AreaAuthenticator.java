package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.Objects;

import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.server.IAuthenticator;
import org.eclipse.moquette.server.RedisPool;
import redis.clients.jedis.Jedis;

/**
 * 创建人：xy
 * 创建时间：15/3/26
 *
 * @version 1.0
 */

public class AreaAuthenticator implements IAuthenticator {
	@Override
	public boolean checkValid(String username, String password) {
		Jedis resource = RedisPool.getPool().getResource();
		String pwd = resource.hget(Constants.KEY_AREA_ACCOUNT, username);
		RedisPool.getPool().returnResource(resource);
		if (Objects.equals(pwd, password)) {
			return true;
		} else {
			return false;
		}
	}
}
