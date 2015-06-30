package org.eclipse.moquette.spi.impl;

import java.nio.ByteBuffer;
import java.util.List;

import com.lmax.disruptor.WorkHandler;
import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.proto.messages.PublishMessage;
import org.eclipse.moquette.server.ServerChannel;
import org.eclipse.moquette.spi.impl.events.OutputMessagingEvent;
import org.eclipse.moquette.spi.impl.events.PublishExtraEvent;
import org.eclipse.moquette.spi.impl.subscriptions.Subscription;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/6/11
 *
 * @version 1.0
 */
@Deprecated
public class PubMsgProcessor implements WorkHandler<ValueEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(PubMsgProcessor.class);

	@Override
	public void onEvent(ValueEvent event) throws Exception {
		try {
			PublishExtraEvent evt = (PublishExtraEvent) event.getEvent();
			long s = System.nanoTime();
			processPublish(evt);
			long e = System.nanoTime() - s;
			LOG.info("matchs takes [{}] ms", e / 1000000);

		} catch (Throwable th) {
			LOG.error("process pub msg error", th);
		} finally {
			event.setEvent(null);
		}

	}

	private void processPublish(PublishExtraEvent evt) {
		LOG.info("PUBLISH from clientID [{}] on topic [{}]", evt.getClientID(), evt.getTopic());
		SubscriptionsStore subscriptions = evt.getSubscriptions();
		String topic = evt.getTopic();
		AbstractMessage.QOSType qos = evt.getQos();
		//TODO:查找该topic订阅者的时间复杂度是O(n),其中n是总共的订阅者。
		List<Subscription> matched = subscriptions.matches(topic);
		ByteBuffer origMessage = evt.getMessage();
//		for (Subscription subscription : matched) {
//			subscription.getClientId()
//		}
		for (final Subscription sub : matched) {
			if (qos.ordinal() > sub.getRequestedQos().ordinal()) {
				qos = sub.getRequestedQos();
			}

			LOG.debug("Broker republishing to client [{}] topic [{}] qos [{}], active {}",
					sub.getClientId(), sub.getTopicFilter(), qos, sub.isActive());
			ByteBuffer message = origMessage.duplicate();
			ProtocolProcessor processor = evt.getProcessor();
			sendPublish(sub.getClientId(), topic, qos, message, false, null, processor, processor.m_clientIDs.get(sub.getClientId()).getSession());
		}
	}

	private void sendPublish(String clientId, String topic, AbstractMessage.QOSType qos, ByteBuffer message, boolean retained, Integer messageID, ProtocolProcessor processor, ServerChannel session) {
		LOG.debug("sendPublish invoked clientId [{}] on topic [{}] QoS {} retained {} messageID {}", clientId, topic, qos, retained, messageID);
		PublishMessage pubMessage = new PublishMessage();
		pubMessage.setRetainFlag(retained);
		pubMessage.setTopicName(topic);
		pubMessage.setQos(qos);
		pubMessage.setPayload(message);

		//set the PacketIdentifier only for QoS > 0
		if (pubMessage.getQos() != AbstractMessage.QOSType.MOST_ONE) {
			pubMessage.setMessageID(messageID);
		} else {
			if (messageID != null) {
				throw new RuntimeException("Internal bad error, trying to forwardPublish a QoS 0 message with PacketIdentifier: " + messageID);
			}
		}

		processor.publishToMainDisruptor(new OutputMessagingEvent(session, pubMessage));
//		session.write(pubMessage);
	}
}
