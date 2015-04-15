package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.Objects;

import cn.thinkjoy.im.common.ClientIds;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.server.IAuthenticator;

/**
 * 创建人：xy
 * 创建时间：15/3/26
 *
 * @version 1.0
 */

public class AreaAuthenticator implements IAuthenticator {
	@Override
	public boolean checkValid(String token, String password, String clientID) {

		String accountArea = null;
		try {
			accountArea = ClientIds.getAccountArea(clientID);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		String pwd = AccountRepository.get(Constants.KEY_AREA_ACCOUNT, accountArea);
		if (null != pwd && Objects.equals(pwd, password)) {
			//TODO 必须再进行token的认证
			return true;
		} else {
			return false;
		}
	}
}
