package org.eclipse.moquette.spi.impl.events;

import java.nio.ByteBuffer;

import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.spi.impl.ProtocolProcessor;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;

/**
 * 创建人：xy
 * 创建时间：15/6/11
 *
 * @version 1.0
 */
@Deprecated
public class PublishExtraEvent extends PublishEvent {
	SubscriptionsStore subscriptions;
	ProtocolProcessor processor;

	public PublishExtraEvent(String topic, AbstractMessage.QOSType qos,
	                         ByteBuffer message, boolean retain, String clientID,
	                         Integer msgID,
	                         SubscriptionsStore subscriptions,
	                         ProtocolProcessor processor) {
		super(topic, qos, message, retain, clientID, msgID);
		this.subscriptions = subscriptions;
		this.processor = processor;
	}

	public SubscriptionsStore getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(SubscriptionsStore subscriptions) {
		this.subscriptions = subscriptions;
	}

	public ProtocolProcessor getProcessor() {
		return processor;
	}

	public void setProcessor(ProtocolProcessor processor) {
		this.processor = processor;
	}
}
