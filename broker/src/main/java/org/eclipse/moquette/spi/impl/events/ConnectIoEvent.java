package org.eclipse.moquette.spi.impl.events;

import cn.thinkjoy.im.api.client.IMClient;

/**
 * 创建人：xy
 * 创建时间：15/6/1
 *
 * @version 1.0
 */
@Deprecated
public class ConnectIoEvent extends IoEvent {

	protected IMClient client;

	public ConnectIoEvent(IoEventType type, String clientID, IMClient client) {
		super(type, clientID);
		this.client = client;
	}

	public IMClient getClient() {
		return client;
	}

	public void setClient(IMClient client) {
		this.client = client;
	}

	@Override
	public String toString() {
		return "ConnectIoEvent{" +
				"type=" + type +
				", clientID='" + clientID + '\'' +
				'}';
	}
}
