package org.eclipse.moquette.spi.impl.events;

import java.util.Set;

import org.eclipse.moquette.spi.impl.subscriptions.Subscription;

/**
 * 创建人：xy
 * 创建时间：15/5/30
 *
 * @version 1.0
 */

/**
 * used for process disconnect, lostconnection, publish event.
 */
@Deprecated
public class ExtraIoEvent extends IoEvent {
	protected Set<Subscription> subscriptions;
	protected String topic;

	public ExtraIoEvent(IoEventType type, String clientID, Set<Subscription> subscriptions, String topic) {
		super(type, clientID);
		this.subscriptions = subscriptions;
		this.topic = topic;
	}

	public Set<Subscription> getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(Set<Subscription> subscriptions) {
		this.subscriptions = subscriptions;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public String toString() {
		return "ExtraIoEvent{" +
				"topic='" + topic + '\'' +
				'}';
	}
}
