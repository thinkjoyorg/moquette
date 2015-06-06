package org.eclipse.moquette.spi.impl.events;

/**
 * 创建人：xy
 * 创建时间：15/5/29
 *
 * @version 1.0
 */


/**
 * base io event
 * <p/>
 * io event used on publish, disconnect, lost connection, subscribe
 *
 * connectIoEvent used on connect
 *
 */
public class IoEvent extends MessagingEvent {
	protected IoEventType type;
	protected String clientID;

	public IoEvent(IoEventType type, String clientID) {
		this.type = type;
		this.clientID = clientID;
	}

	public IoEventType getType() {
		return type;
	}

	public void setType(IoEventType type) {
		this.type = type;
	}

	public String getClientID() {
		return clientID;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	@Override
	public String toString() {
		return "IoEvent{" +
				"clientID='" + clientID + '\'' +
				", type=" + type +
				'}';
	}

	public enum IoEventType {
		CONNECT, DISCONNECT, LOSTCONNECTION, SUBSCRIBE, PUBLISH
	}

}
