/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.eclipse.moquette.spi.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.thinkjoy.im.common.ClientIds;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.proto.messages.*;
import org.eclipse.moquette.proto.messages.AbstractMessage.QOSType;
import org.eclipse.moquette.server.ConnectionDescriptor;
import org.eclipse.moquette.server.IAuthenticator;
import org.eclipse.moquette.server.ServerChannel;
import org.eclipse.moquette.server.netty.NettyChannel;
import org.eclipse.moquette.spi.IMessagesStore;
import org.eclipse.moquette.spi.ISessionsStore;
import org.eclipse.moquette.spi.impl.events.*;
import org.eclipse.moquette.spi.impl.subscriptions.Subscription;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;
import org.eclipse.moquette.spi.impl.thinkjoy.TopicRouterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.moquette.parser.netty.Utils.VERSION_3_1;
import static org.eclipse.moquette.parser.netty.Utils.VERSION_3_1_1;

/**
 * Class responsible to handle the logic of MQTT protocol it's the director of
 * the protocol execution.
 *
 * Used by the front facing class SimpleMessaging.
 *
 * @author andrea
 */

/**
 * 协议处理器将事件分为io(connect, disconnect, lost connection)事件和非io事件。
 * io事件使用workerpool模型处理
 * 非io事件使用单线程worker模型处理
 */
public class ProtocolProcessor implements EventHandler<ValueEvent> {

	private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessor.class);

	public ConcurrentHashMapV8<String, ConnectionDescriptor> m_clientIDs = new ConcurrentHashMapV8<>();
	private SubscriptionsStore subscriptions;
	private IMessagesStore m_messagesStore;
	private ISessionsStore m_sessionsStore;
	private IAuthenticator m_authenticator;
	private Lock lock = new ReentrantLock();
	//maps clientID to Will testament, if specified on CONNECT
	private Map<String, WillMessage> m_willStore = new HashMap<>();
	private ExecutorService mainExecutor,
			ioExecutor;
	//pubMsgExecutor;

	private RingBuffer<ValueEvent> mainRingBuffer,
			ioRingBuffer;
	//pubMsgRingBuffer;

	private Disruptor<ValueEvent> mainDisruptor,
			ioDisruptor;
	//pubMsgDisruptor;

	ProtocolProcessor() {
	}

	/**
	 * @param subscriptions  the subscription store where are stored all the existing
	 *                       clients subscriptions.
	 * @param storageService the persistent store to use for save/load of messages
	 *                       for QoS1 and QoS2 handling.
	 * @param sessionsStore  the clients sessions store, used to persist subscriptions.
	 * @param authenticator  the authenticator used in connect messages
	 */
	void init(SubscriptionsStore subscriptions, IMessagesStore storageService,
	          ISessionsStore sessionsStore,
	          IAuthenticator authenticator) {
		//m_clientIDs = clientIDs;
		this.subscriptions = subscriptions;
		LOG.debug("subscription tree on init {}", subscriptions.dumpTree());
		m_authenticator = authenticator;
		m_messagesStore = storageService;
		m_sessionsStore = sessionsStore;

		//init the output ringbuffer
		initExecutor();

		initDisrutor();

		startDisruptor();

		// Get the ring buffer from the Disruptor to be used for publishing.
		initRingBuffer();

	}

	private void startDisruptor() {
		mainDisruptor.handleEventsWith(this);
		mainDisruptor.start();

		ioDisruptor.handleEventsWithWorkerPool(create(Constants.TYPE_PROCESSOR_IO));
		ioDisruptor.start();

//		pubMsgDisruptor.handleEventsWithWorkerPool(create(Constants.TYPE_PROCESSOR_PUBMSG));
//		pubMsgDisruptor.start();
	}

	private void initRingBuffer() {
		mainRingBuffer = mainDisruptor.getRingBuffer();
		ioRingBuffer = ioDisruptor.getRingBuffer();
//		pubMsgRingBuffer = pubMsgDisruptor.getRingBuffer();
	}

	private void initDisrutor() {
		mainDisruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, mainExecutor);
		ioDisruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, ioExecutor);
//		pubMsgDisruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, pubMsgExecutor);
	}

	private void initExecutor() {
		mainExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("ProtocolProcessor"));
		ioExecutor = Executors.newFixedThreadPool(Constants.wThreads + 1, new DefaultThreadFactory("IoTaskProcessor"));
		/**
		 *
		 * 由于在匹配订阅者(SubscriptionsStore.matchs(String topic))的过程中存在瓶颈
		 * 所以将发送消息，改为多线程模型。
		 *
		 */
		//pubMsgExecutor = Executors.newFixedThreadPool(Constants.nThreads + 1, new DefaultThreadFactory("PubMsgProcessor"));
	}

	void destory() {
		try {
			ioExecutor.shutdown();
			mainExecutor.shutdown();

			mainDisruptor.shutdown();
			ioDisruptor.shutdown();

			m_messagesStore.close();

			IoTaskProcessor.closeIMClient();

		} catch (Throwable th) {
			LOG.warn("destory error", th);
		}

	}

	private final WorkHandler[] create(int processorType) {
		int size = Constants.wThreads + 1;
		WorkHandler[] handlers = new WorkHandler[size];
		for (int i = 0; i < handlers.length; i++) {
			// always TYPE_PROCESSOR_IO
			if (processorType == Constants.TYPE_PROCESSOR_IO) {
				handlers[i] = new IoTaskProcessor();
			}
		}
		return handlers;
	}

	@MQTTMessage(message = ConnectMessage.class)
	void processConnect(ServerChannel session, final ConnectMessage msg) {
		LOG.debug("CONNECT for client [{}]", msg.getClientID());
		if (msg.getProcotolVersion() != VERSION_3_1 && msg.getProcotolVersion() != VERSION_3_1_1) {
			ConnAckMessage badProto = new ConnAckMessage();
			badProto.setReturnCode(ConnAckMessage.UNNACEPTABLE_PROTOCOL_VERSION);
			LOG.warn("processConnect sent bad proto ConnAck");
			session.write(badProto);
			session.close(false);
			return;
		}

		if (msg.getClientID() == null || msg.getClientID().length() == 0) {
			ConnAckMessage okResp = new ConnAckMessage();
			okResp.setReturnCode(ConnAckMessage.IDENTIFIER_REJECTED);
			session.write(okResp);
			session.close(false);
			return;
		}
		//handle user authentication
		if (!ClientIds.isValid(msg.getClientID())) {
			authFail(session);
		}
		if (msg.isUserFlag()) {
			String pwd = null;
			if (msg.isPasswordFlag()) {
				pwd = msg.getPassword();
			}
			//这里的username是业务方传过来的token。
			if (!m_authenticator.checkValid(msg.getUsername(), pwd, msg.getClientID())) {
				authFail(session);
			}
		} else {
			authFail(session);
		}

		//if an old client with the same ID already exists close its session.
		if (m_clientIDs.containsKey(msg.getClientID())) {
			LOG.info("Found an existing connection with same client ID [{}], forcing to close", msg.getClientID());
			//clean the subscriptions if the old used a cleanSession = true
			ServerChannel oldSession = m_clientIDs.get(msg.getClientID()).getSession();
			boolean cleanSession = (Boolean) oldSession.getAttribute(NettyChannel.ATTR_KEY_CLEANSESSION);
			if (cleanSession) {
				//cleanup topic subscriptions
				cleanSession(msg.getClientID());
			}

			oldSession.close(false);


			LOG.debug("Existing connection with same client ID [{}], forced to close", msg.getClientID());
		}

		IoEvent ioEvent = new IoEvent(IoEvent.IoEventType.CONNECT, msg.getClientID());
		publishToIoDisruptor(ioEvent);

		ConnectionDescriptor connDescr = new ConnectionDescriptor(msg.getClientID(), session, msg.isCleanSession());
		m_clientIDs.put(msg.getClientID(), connDescr);

		int keepAlive = msg.getKeepAlive();
//		LOG.trace("Connect with keepAlive {} s", keepAlive);
		session.setAttribute(NettyChannel.ATTR_KEY_KEEPALIVE, keepAlive);
		session.setAttribute(NettyChannel.ATTR_KEY_CLEANSESSION, msg.isCleanSession());
		//used to track the client in the subscription and publishing phases.
		session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, msg.getClientID());

		session.setIdleTime(Math.round(keepAlive * 1.5f));

		//Handle will flag
		//TODO:去掉will功能
//        if (msg.isWillFlag()) {
//            AbstractMessage.QOSType willQos = AbstractMessage.QOSType.values()[msg.getWillQos()];
//            byte[] willPayload = msg.getWillMessage().getBytes();
//            ByteBuffer bb = (ByteBuffer) ByteBuffer.allocate(willPayload.length).put(willPayload).flip();
//            //save the will testment in the clientID store
//            WillMessage will = new WillMessage(msg.getWillTopic(), bb, msg.isWillRetain(),willQos );
//            m_willStore.put(msg.getClientID(), will);
//        }

//        subscriptions.activate(msg.getClientID());

		//handle clean session flag
		//TODO:目前没必要进行清理，因为每次的clientID都不相同
//		if (msg.isCleanSession()) {
//			//remove all prev subscriptions
//			//cleanup topic subscriptions
//			cleanSession(msg.getClientID());
//		}

		ConnAckMessage okResp = new ConnAckMessage();
		okResp.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
		//TODO:此版本不需要此功能
//		if (!msg.isCleanSession() && m_sessionsStore.contains(msg.getClientID())) {
//			okResp.setSessionPresent(true);
//		}
		session.write(okResp);

		m_sessionsStore.addNewSubscription(Subscription.createEmptySubscription(msg.getClientID(), true), msg.getClientID()); //null means EmptySubscription
		LOG.info("Connected client ID [{}] with clean session {}", msg.getClientID(), msg.isCleanSession());
		//TODO:此版本中不需要推送离线消息。
//	    if (!msg.isCleanSession()) {
//		    //force the republish of stored QoS1 and QoS2
//            republishStoredInSession(msg.getClientID());
//        }
//
	}

	//mqtt 认证失败
	private final void authFail(ServerChannel session) {
		ConnAckMessage okResp = new ConnAckMessage();
		okResp.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
		session.write(okResp);
		session.close(false);
		return;
	}

	/**
	 * Republish QoS1 and QoS2 messages stored into the session for the clientID.
	 */
	private void republishStoredInSession(String clientID) {
		LOG.trace("republishStoredInSession for client [{}]", clientID);
		List<PublishEvent> publishedEvents = m_messagesStore.listMessagesInSession(clientID);
		if (publishedEvents.isEmpty()) {
			LOG.info("No stored messages for client [{}]", clientID);
			return;
		}

		LOG.info("republishing stored messages to client [{}]", clientID);
		for (PublishEvent pubEvt : publishedEvents) {
			sendPublish(pubEvt.getClientID(), pubEvt.getTopic(), pubEvt.getQos(),
					pubEvt.getMessage(), false, pubEvt.getMessageID());
			m_messagesStore.removeMessageInSession(clientID, pubEvt.getMessageID());
		}
	}

	@MQTTMessage(message = PubAckMessage.class)
	void processPubAck(ServerChannel session, PubAckMessage msg) {
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		int messageID = msg.getMessageID();
		//Remove the message from message store
		m_messagesStore.removeMessageInSession(clientID, messageID);
	}

	private void cleanSession(String clientID) {
		LOG.info("cleaning old saved subscriptions for client [{}]", clientID);
		//remove from log all subscriptions
		lock.lock();
		try {
			m_sessionsStore.wipeSubscriptions(clientID);
			subscriptions.removeForClient(clientID);

			//remove also the messages stored of type QoS1/2
			m_messagesStore.dropMessagesInSession(clientID);

		} finally {
			lock.unlock();
		}
	}

	private void cleanSessionWithoutLock(String clientID) {
//		LOG.info("cleaning old saved subscriptions for client [{}]", clientID);
		//remove from log all subscriptions
		m_sessionsStore.wipeSubscriptions(clientID);
		subscriptions.removeForClient(clientID);

		//remove also the messages stored of type QoS1/2
		m_messagesStore.dropMessagesInSession(clientID);


	}

	@MQTTMessage(message = PublishMessage.class)
	void processPublish(ServerChannel session, PublishMessage msg) {
		LOG.trace("PUB --PUBLISH--> SRV processPublish invoked with {}", msg);
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		final String topic = msg.getTopicName();
		final AbstractMessage.QOSType qos = msg.getQos();
		final ByteBuffer message = msg.getPayload();
		boolean retain = msg.isRetainFlag();
		processPublish(clientID, topic, qos, message, retain, msg.getMessageID());
//		PublishExtraEvent publishExtraEvent = new PublishExtraEvent(topic, qos, message, retain,
//				clientID, msg.getMessageID(),
//				subscriptions, this);
//		publishToPubMsgDisruptor(publishExtraEvent);
	}

	private void processPublish(String clientID, String topic, QOSType qos, ByteBuffer message, boolean retain, Integer messageID) {
		LOG.info("PUBLISH from clientID [{}] on topic [{}] with QoS [{}]", clientID, topic, qos);

		if (qos == AbstractMessage.QOSType.MOST_ONE) { //QoS0
			forward2Subscribers(topic, qos, message, retain, messageID);
		} else if (qos == AbstractMessage.QOSType.LEAST_ONE) {
			PublishEvent inFlightEvt = new PublishEvent(topic, qos, message, retain,
					clientID, messageID);
			//TODO use a message store for TO PUBLISH MESSAGES it has nothing to do with inFlight!!
			m_messagesStore.addInFlight(inFlightEvt, clientID, messageID);
			forward2Subscribers(topic, qos, message, retain, messageID);
			m_messagesStore.cleanInFlight(clientID, messageID);
			//NB the PUB_ACK could be sent also after the addInFlight
			sendPubAck(new PubAckEvent(messageID, clientID));
			LOG.debug("replying with PubAck to MSG ID {}", messageID);
		} else if (qos == AbstractMessage.QOSType.EXACTLY_ONCE) {
			String publishKey = String.format("%s%d", clientID, messageID);
			//store the message in temp store
			PublishEvent qos2Persistent = new PublishEvent(topic, qos, message, retain,
					clientID, messageID);
			m_messagesStore.persistQoS2Message(publishKey, qos2Persistent);
			sendPubRec(clientID, messageID);
			//Next the client will send us a pub rel
			//NB publish to subscribers for QoS 2 happen upon PUBREL from publisher
		}

		if (retain) {
			if (qos == AbstractMessage.QOSType.MOST_ONE) {
				//QoS == 0 && retain => clean old retained
				m_messagesStore.cleanRetained(topic);
			} else {
				m_messagesStore.storeRetained(topic, message, qos);
			}
		}
	}

	/**
	 * Specialized version to publish will testament message.
	 */
	private void forwardPublishWill(WillMessage will, String clientID) {
		//it has just to publish the message downstream to the subscribers
		final String topic = will.getTopic();
		final AbstractMessage.QOSType qos = will.getQos();
		final ByteBuffer message = will.getPayload();
		boolean retain = will.isRetained();
		//NB it's a will publish, it needs a PacketIdentifier for this conn, default to 1
		if (qos == AbstractMessage.QOSType.MOST_ONE) {
			forward2Subscribers(topic, qos, message, retain, null);
		} else {
			int messageId = m_messagesStore.nextPacketID(clientID);
			forward2Subscribers(topic, qos, message, retain, messageId);
		}

	}

	/**
	 * Flood the subscribers with the message to notify. MessageID is optional and should only used for QoS 1 and 2
	 */
	void forward2Subscribers(String topic, AbstractMessage.QOSType pubQos, ByteBuffer origMessage,
	                                 boolean retain, Integer messageID) {
		LOG.debug("forward2Subscribers republishing to existing subscribers that matches the topic {}", topic);
//		if (LOG.isDebugEnabled()) {
//			LOG.debug("content [{}]", DebugUtils.payload2Str(origMessage));
//			LOG.debug("subscription tree {}", subscriptions.dumpTree());
//		}

		List<Subscription> matched = subscriptions.matches(topic);
//		for (Subscription subscription : matched) {
//			subscription.getClientId()
//		}
		for (final Subscription sub : matched) {
			AbstractMessage.QOSType qos = pubQos;
			if (qos.ordinal() > sub.getRequestedQos().ordinal()) {
				qos = sub.getRequestedQos();
			}

			LOG.debug("Broker republishing to client [{}] topic [{}] qos [{}], active {}",
					sub.getClientId(), sub.getTopicFilter(), qos, sub.isActive());
			ByteBuffer message = origMessage.duplicate();
			if (qos == AbstractMessage.QOSType.MOST_ONE && sub.isActive()) {
				//QoS 0
				//forwardPublishQoS0(sub.getClientId(), topic, qos, message, false);
				sendPublish(sub.getClientId(), topic, qos, message, false, null);
			} else {
				//QoS 1 or 2
				//if the target subscription is not clean session and is not connected => store it
				if (!sub.isCleanSession() && !sub.isActive()) {
					//clone the event with matching clientID
					PublishEvent newPublishEvt = new PublishEvent(topic, qos, message, retain, sub.getClientId(), messageID != null ? messageID : 0);
					m_messagesStore.storePublishForFuture(newPublishEvt);
				} else {
					//TODO also QoS 1 has to be stored in Flight Zone
					//if QoS 2 then store it in temp memory
					if (qos == AbstractMessage.QOSType.EXACTLY_ONCE) {
						PublishEvent newPublishEvt = new PublishEvent(topic, qos, message, retain, sub.getClientId(), messageID != null ? messageID : 0);
						m_messagesStore.addInFlight(newPublishEvt, sub.getClientId(), messageID);
					}
					//publish
					//TODO:去掉离线功能
//                    if (sub.isActive()) {
//	                    int messageId = m_messagesStore.nextPacketID(sub.getClientId());
//	                    sendPublish(sub.getClientId(), topic, qos, message, false, messageId);
//                    }
					int messageId = m_messagesStore.nextPacketID(sub.getClientId());
					sendPublish(sub.getClientId(), topic, qos, message, false, messageId);
				}
			}
		}
	}

	private void sendPublish(String clientId, String topic, AbstractMessage.QOSType qos, ByteBuffer message, boolean retained, Integer messageID) {
		LOG.debug("sendPublish invoked clientId [{}] on topic [{}] QoS {} retained {} messageID {}", clientId, topic, qos, retained, messageID);
		PublishMessage pubMessage = new PublishMessage();
		pubMessage.setRetainFlag(retained);
		pubMessage.setTopicName(topic);
		pubMessage.setQos(qos);
		pubMessage.setPayload(message);

//        LOG.info("send publish message to [{}] on topic [{}] content [{}]", clientId, topic, DebugUtils.payload2Str(message));

		//set the PacketIdentifier only for QoS > 0
		if (pubMessage.getQos() != AbstractMessage.QOSType.MOST_ONE) {
			pubMessage.setMessageID(messageID);
		} else {
			if (messageID != null) {
				throw new RuntimeException("Internal bad error, trying to forwardPublish a QoS 0 message with PacketIdentifier: " + messageID);
			}
		}

		if (m_clientIDs == null) {
			throw new RuntimeException("Internal bad error, found m_clientIDs to null while it should be initialized, somewhere it's overwritten!!");
		}
//		LOG.debug("clientIDs are {}", m_clientIDs);
		if (m_clientIDs.get(clientId) == null) {
			LOG.error("Can't find a ConnectionDescriptor for client [{}] in cache", clientId);
			throw new RuntimeException(String.format("Can't find a ConnectionDescriptor for client <%s> in cache", clientId));
		}
		LOG.debug("Session for clientId {} is {}", clientId, m_clientIDs.get(clientId).getSession());
//        m_clientIDs.get(clientId).getSession().write(pubMessage);

		publishToMainDisruptor(new OutputMessagingEvent(m_clientIDs.get(clientId).getSession(), pubMessage));
	}

	private void sendPubRec(String clientID, int messageID) {
		LOG.trace("PUB <--PUBREC-- SRV sendPubRec invoked for clientID {} with messageID {}", clientID, messageID);
		PubRecMessage pubRecMessage = new PubRecMessage();
		pubRecMessage.setMessageID(messageID);

//        m_clientIDs.get(clientID).getSession().write(pubRecMessage);
		publishToMainDisruptor(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubRecMessage));
	}

	private void sendPubAck(PubAckEvent evt) {
		LOG.trace("sendPubAck invoked");

		String clientId = evt.getClientID();

		PubAckMessage pubAckMessage = new PubAckMessage();
		pubAckMessage.setMessageID(evt.getMessageId());

		try {
			if (m_clientIDs == null) {
				throw new RuntimeException("Internal bad error, found m_clientIDs to null while it should be initialized, somewhere it's overwritten!!");
			}
//			LOG.debug("clientIDs are {}", m_clientIDs);
			if (m_clientIDs.get(clientId) == null) {
				LOG.error("Can't find a ConnectionDescriptor for client [{}] in cache", clientId);
				throw new RuntimeException(String.format("Can't find a ConnectionDescriptor for client %s in cache", clientId));
			}
//            LOG.debug("Session for clientId " + clientId + " is " + m_clientIDs.get(clientId).getSession());
//            m_clientIDs.get(clientId).getSession().write(pubAckMessage);
			publishToMainDisruptor(new OutputMessagingEvent(m_clientIDs.get(clientId).getSession(), pubAckMessage));
		} catch (Throwable t) {
			LOG.error(null, t);
		}
	}

	/**
	 * Second phase of a publish QoS2 protocol, sent by publisher to the broker. Search the stored message and publish
	 * to all interested subscribers.
	 */
	@MQTTMessage(message = PubRelMessage.class)
	void processPubRel(ServerChannel session, PubRelMessage msg) {
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		int messageID = msg.getMessageID();
		LOG.debug("PUB --PUBREL--> SRV processPubRel invoked for clientID {} ad messageID {}", clientID, messageID);
		String publishKey = String.format("%s%d", clientID, messageID);
		PublishEvent evt = m_messagesStore.retrieveQoS2Message(publishKey);

		final String topic = evt.getTopic();
		final AbstractMessage.QOSType qos = evt.getQos();

		forward2Subscribers(topic, qos, evt.getMessage(), evt.isRetain(), evt.getMessageID());

		m_messagesStore.removeQoS2Message(publishKey);

		if (evt.isRetain()) {
			m_messagesStore.storeRetained(topic, evt.getMessage(), qos);
		}

		sendPubComp(clientID, messageID);
	}

	private void sendPubComp(String clientID, int messageID) {
		LOG.debug("PUB <--PUBCOMP-- SRV sendPubComp invoked for clientID {} ad messageID {}", clientID, messageID);
		PubCompMessage pubCompMessage = new PubCompMessage();
		pubCompMessage.setMessageID(messageID);

//        m_clientIDs.get(clientID).getSession().write(pubCompMessage);
		publishToMainDisruptor(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubCompMessage));
	}

	@MQTTMessage(message = PubRecMessage.class)
	void processPubRec(ServerChannel session, PubRecMessage msg) {
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		int messageID = msg.getMessageID();
		//once received a PUBREC reply with a PUBREL(messageID)
		LOG.debug("\t\tSRV <--PUBREC-- SUB processPubRec invoked for clientID {} ad messageID {}", clientID, messageID);
		PubRelMessage pubRelMessage = new PubRelMessage();
		pubRelMessage.setMessageID(messageID);
		pubRelMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);

//        m_clientIDs.get(clientID).getSession().write(pubRelMessage);
		//publishToMainDisruptor(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubRelMessage));
		session.write(pubRelMessage);
	}

	@MQTTMessage(message = PubCompMessage.class)
	void processPubComp(ServerChannel session, PubCompMessage msg) {
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		int messageID = msg.getMessageID();
		LOG.debug("\t\tSRV <--PUBCOMP-- SUB processPubComp invoked for clientID {} ad messageID {}", clientID, messageID);
		//once received the PUBCOMP then remove the message from the temp memory
		m_messagesStore.cleanInFlight(clientID, messageID);
	}

	@MQTTMessage(message = DisconnectMessage.class)
	void processDisconnect(final ServerChannel session, DisconnectMessage msg) throws InterruptedException {
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		boolean cleanSession = (Boolean) session.getAttribute(NettyChannel.ATTR_KEY_CLEANSESSION);
		Set<Subscription> subscription = m_sessionsStore.getSubscriptionById(clientID);
		ExtraIoEvent extraIoEvent = new ExtraIoEvent(IoEvent.IoEventType.DISCONNECT, clientID, subscription);

		publishToIoDisruptor(extraIoEvent);

		if (cleanSession) {

			cleanSessionWithoutLock(clientID);
		}
		m_clientIDs.remove(clientID);
		session.close(true);

		LOG.info("DISCONNECT client [{}] with clean session {}", clientID, cleanSession);

	}

	void processConnectionLost(final LostConnectionEvent evt) {
		String clientID = evt.clientID;

		//如果丢失连接必须也要清理客户端信息。
		Set<Subscription> subscription = m_sessionsStore.getSubscriptionById(clientID);
		ExtraIoEvent extraIoEvent = new ExtraIoEvent(IoEvent.IoEventType.LOSTCONNECTION, clientID, subscription);
		publishToIoDisruptor(extraIoEvent);

		//If already removed a disconnect message was already processed for this clientID
		if (m_clientIDs.remove(clientID) != null) {
			cleanSessionWithoutLock(clientID);
		}

		LOG.info("Lost connection with client [{}]", clientID);

	}

	/**
	 * Remove the clientID from topic subscription, if not previously subscribed,
	 * doesn't reply any error
	 */
	@MQTTMessage(message = UnsubscribeMessage.class)
	void processUnsubscribe(ServerChannel session, UnsubscribeMessage msg) {
		List<String> topics = msg.topicFilters();
		int messageID = msg.getMessageID();
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		LOG.info("UNSUBSCRIBE subscription on topics {} for clientID [{}]", topics, clientID);
		for (String topic : topics) {
			subscriptions.removeSubscription(topic, clientID);
			m_sessionsStore.removeSubscription(topic, clientID);
			TopicRouterRepository.clean(topic);
		}
		//ack the client
		UnsubAckMessage ackMessage = new UnsubAckMessage();
		ackMessage.setMessageID(messageID);

		LOG.debug("replying with UnsubAck to MSG ID {}", messageID);
		session.write(ackMessage);
	}

	@MQTTMessage(message = SubscribeMessage.class)
	void processSubscribe(ServerChannel session, SubscribeMessage msg) {
		String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
		boolean cleanSession = (Boolean) session.getAttribute(NettyChannel.ATTR_KEY_CLEANSESSION);
		LOG.debug("SUBSCRIBE client [{}] packetID {}", clientID, msg.getMessageID());

		//ack the client
		SubAckMessage ackMessage = new SubAckMessage();
		ackMessage.setMessageID(msg.getMessageID());

		//reply with requested qos
		for (SubscribeMessage.Couple req : msg.subscriptions()) {
			AbstractMessage.QOSType qos = AbstractMessage.QOSType.values()[req.getQos()];
			Subscription newSubscription = new Subscription(clientID, req.getTopicFilter(), qos, cleanSession);
			boolean valid = subscribeSingleTopic(newSubscription, req.getTopicFilter());

			ackMessage.addType(valid ? qos : QOSType.FAILURE);
		}

		LOG.debug("SUBACK for packetID {}", msg.getMessageID());
		session.write(ackMessage);
	}

	private boolean subscribeSingleTopic(Subscription newSubscription, final String topic) {
		LOG.info("[{}] subscribed to topic [{}] with QoS {}",
				newSubscription.getClientId(), topic,
				AbstractMessage.QOSType.formatQoS(newSubscription.getRequestedQos()));
		String clientID = newSubscription.getClientId();
		boolean validTopic = SubscriptionsStore.validate(newSubscription);
		if (!validTopic) {
			return false;
		}

		m_sessionsStore.addNewSubscription(newSubscription, clientID);
		subscriptions.add(newSubscription);

		TopicRouterRepository.add(topic);

		return true;

	}

	public void publishToMainDisruptor(MessagingEvent msgEvent) {
		LOG.debug("publishToMainDisruptor publishing event on output {}", msgEvent);
		long sequence = mainRingBuffer.next();
		ValueEvent event = mainRingBuffer.get(sequence);

		event.setEvent(msgEvent);

		mainRingBuffer.publish(sequence);
	}

	private void publishToIoDisruptor(MessagingEvent msgEvent) {
		LOG.debug("publishToIoDisruptor publishing event on output {}", msgEvent);
		long sequence = ioRingBuffer.next();
		ValueEvent event = ioRingBuffer.get(sequence);

		event.setEvent(msgEvent);

		ioRingBuffer.publish(sequence);
	}

//	private void publishToPubMsgDisruptor(MessagingEvent msgEvent) {
//		LOG.debug("publishToPubMsgDisruptor publishing event on output {}", msgEvent);
//		long sequence = pubMsgRingBuffer.next();
//		ValueEvent event = pubMsgRingBuffer.get(sequence);
//
//		event.setEvent(msgEvent);
//
//		pubMsgRingBuffer.publish(sequence);
//	}


	public void onEvent(ValueEvent t, long l, boolean bln) throws Exception {
//		if (LOG.isDebugEnabled()) {
//			long p = mainRingBuffer.getCursor();
//			long backlog = Utils.count(p, l);
//			LOG.debug("mainDisruptor has [{}] backlog ---------", backlog);
//		}
		try {
			MessagingEvent evt = t.getEvent();
			//always OutputMessagingEvent instance
			OutputMessagingEvent outEvent = (OutputMessagingEvent) evt;
			LOG.debug("Output event, sending {}", outEvent.getMessage());
			outEvent.getChannel().write(outEvent.getMessage());
		}finally {
			t.setEvent(null);
		}

	}

	static final class WillMessage {
		private final String topic;
		private final ByteBuffer payload;
		private final boolean retained;
		private final QOSType qos;

		public WillMessage(String topic, ByteBuffer payload, boolean retained, QOSType qos) {
			this.topic = topic;
			this.payload = payload;
			this.retained = retained;
			this.qos = qos;
		}

		public String getTopic() {
			return topic;
		}

		public ByteBuffer getPayload() {
			return payload;
		}

		public boolean isRetained() {
			return retained;
		}

		public QOSType getQos() {
			return qos;
		}

	}

}
