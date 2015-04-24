package org.eclipse.moquette.spi.impl.thinkjoy;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import cn.thinkjoy.cloudstack.dynconfig.DynConfigClient;
import cn.thinkjoy.cloudstack.dynconfig.DynConfigClientFactory;
import cn.thinkjoy.im.common.ClientIds;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.squareup.okhttp.ConnectionPool;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.server.IAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/3/26
 *
 * @version 1.0
 */

public class AreaAuthenticator implements IAuthenticator {
	private static final Logger LOGGER = LoggerFactory.getLogger(AreaAuthenticator.class);
	private final OkHttpClient httpClient;
	private final DynConfigClient client;

	public AreaAuthenticator() {
		client = DynConfigClientFactory.getClient();
		httpClient = new OkHttpClient();
		try {
			client.init();
			httpClient.setConnectTimeout(10, TimeUnit.SECONDS);
			httpClient.setConnectionPool(ConnectionPool.getDefault());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	@Override
	public boolean checkValid(String token, String password, String clientID) {

		String accountArea = null;
		try {
			accountArea = ClientIds.getAccountArea(clientID);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return false;
		}
		String pwd = AccountRepository.get(Constants.KEY_AREA_ACCOUNT, accountArea);
		if (null != pwd && Objects.equals(pwd, password)) {
			if (authToken(token)) {
				return true;
			}
		}
		return false;
	}

	private boolean authToken(String token) {
		boolean b = TokenRepository.authToken(token);
		if (!b) {

			try {
				String url = client.getConfig("im-service", "common", "httpTokenAuthURL");
				String urlWithParam = new StringBuilder(url).append("?").append("token=").append(token).toString();
				Request request = new Request.Builder()
						.url(urlWithParam)
						.build();
				Response response = httpClient.newCall(request).execute();
				String body = response.body().string();

				/**
				 * token认证协议：
				 * {"state":"调用状态，1.成功，其他.失败","msg":"描述","dt":"时间戳","data":{"valid":"是否有效，true或false","ttl":"该token的生存时间们，单位是秒"}}
				 */
				JSONObject data = JSON.parseObject(body).getJSONObject("data");
				String valid = data.getString("valid");
				if ("true".equalsIgnoreCase(valid)) {
					long ttl = data.getLong("ttl");
					TokenRepository.set(token, ttl);
					return true;
				}
			} catch (Exception e) {

				return false;
			}
		}

		return b;
	}
}
