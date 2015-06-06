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

public class ExtraIoEvent extends IoEvent {

	protected Set<Subscription> subscriptions;

	public ExtraIoEvent(IoEventType type, String clientID, Set<Subscription> subscriptions) {
		super(type, clientID);
		this.subscriptions = subscriptions;
	}

	public Set<Subscription> getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(Set<Subscription> subscriptions) {
		this.subscriptions = subscriptions;
	}

	@Override
	public String toString() {
		return "ExtraIoEvent{" +
				"type=" + type +
				'}';
	}
}
