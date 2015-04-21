package org.eclipse.moquette.spi.impl.thinkjoy;

/**
 * Created by Michael on 4/19/15.
 * <p/>
 * 系统指令
 * 通知用户被踢下线，在客户端的sdk中进行处理，断开网络，并且提示reason信息
 */
public class KickOrder extends SystemOrder {
	private final String BIZ_SYS = "im-sys";
	private final String BIZ_TYPE = "kick";
	/**
	 * 被踢的终端id
	 */
	private String clientId;
	private String reason;

	public KickOrder() {
	}

	public KickOrder(String from, String to, String clientId, String reason) {
		this.clientId = clientId;
		this.from = from;
		this.reason = reason;
		this.to = to;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}


	@Override
	public String pickBizSys() {
		return BIZ_SYS;
	}

	@Override
	public String pickBizType() {
		return BIZ_TYPE;
	}

}
