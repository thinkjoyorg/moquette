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

import cn.thinkjoy.im.api.client.IMClient;
import cn.thinkjoy.im.common.ClientIds;
import cn.thinkjoy.im.protocol.system.KickOrder;
import com.google.common.collect.Sets;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.proto.messages.*;
import org.eclipse.moquette.proto.messages.AbstractMessage.QOSType;
import org.eclipse.moquette.server.ConnectionDescriptor;
import org.eclipse.moquette.server.IAuthenticator;
import org.eclipse.moquette.server.ServerChannel;
import org.eclipse.moquette.server.netty.NettyChannel;
import org.eclipse.moquette.spi.IMessagesStore;
import org.eclipse.moquette.spi.ISessionsStore;
import org.eclipse.moquette.spi.impl.events.LostConnectionEvent;
import org.eclipse.moquette.spi.impl.events.MessagingEvent;
import org.eclipse.moquette.spi.impl.events.OutputMessagingEvent;
import org.eclipse.moquette.spi.impl.subscriptions.Subscription;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;
import org.eclipse.moquette.spi.impl.thinkjoy.OnlineStateRepository;
import org.eclipse.moquette.spi.impl.thinkjoy.TopicRouterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
 * io事件使用异步模型处理 future + Listener
 */
public class ProtocolProcessor implements EventHandler<ValueEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessor.class);
    private static IMClient client = null;

    static {
        client = IMClient.get();
        try {
            client.prepare();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    private Map<String, ConnectionDescriptor> m_clientIDs = new HashMap<>();
    private SubscriptionsStore subscriptions;
    private IMessagesStore m_messagesStore;
    private ISessionsStore m_sessionsStore;
    private IAuthenticator m_authenticator;
    private ExecutorService mainExecutor;
    private RingBuffer<ValueEvent> mainRingBuffer;
    private Disruptor<ValueEvent> mainDisruptor;
    private DefaultEventExecutorGroup taskExecutors = new DefaultEventExecutorGroup(Constants.wThreads, new DefaultThreadFactory("TaskExecutor"));

    private boolean flag = false;

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
    }

    private void initRingBuffer() {
        mainRingBuffer = mainDisruptor.getRingBuffer();
    }

    private void initDisrutor() {
        mainDisruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, mainExecutor);
    }

    private void initExecutor() {
        mainExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("ProtocolProcessor"));
    }

    void destory() {
        try {
            mainExecutor.shutdown();

            mainDisruptor.shutdown();

            m_messagesStore.close();

            client.shutdown();

        } catch (Throwable th) {
            LOG.warn("destory error", th);
        }

    }


    @MQTTMessage(message = ConnectMessage.class)
    void processConnect(final ServerChannel session, final ConnectMessage msg) {
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
            LOG.error("clientID can not be null");
            ConnAckMessage okResp = new ConnAckMessage();
            okResp.setReturnCode(ConnAckMessage.IDENTIFIER_REJECTED);
            session.write(okResp);
            session.close(false);
            return;
        }
//		handle user authentication
        if (!ClientIds.isValid(msg.getClientID())) {
            LOG.error("invalid clientID[{}]", msg.getClientID());
            authFail(session);
        }
        if (msg.isUserFlag() && msg.isPasswordFlag()) {
            authAsync(session, msg.getUsername(), msg.getPassword(), msg.getClientID());

        } else {
            LOG.error("userFlag and passwordFlag must give");
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
            //说明客户端是由于网络波动造成的连接丢失
            //那么服务端不清理clientID对应的连接
            flag = true;

            LOG.debug("Existing connection with same client ID [{}], forced to close", msg.getClientID());
        }

        connectIntervalAsync(msg.getClientID());

        ConnectionDescriptor connDescr = new ConnectionDescriptor(msg.getClientID(), session, msg.isCleanSession());
        m_clientIDs.put(msg.getClientID(), connDescr);

        int keepAlive = msg.getKeepAlive();
//		LOG.trace("Connect with keepAlive {} s", keepAlive);
        session.setAttribute(NettyChannel.ATTR_KEY_KEEPALIVE, keepAlive);
        session.setAttribute(NettyChannel.ATTR_KEY_CLEANSESSION, msg.isCleanSession());
        //used to track the client in the subscription and publishing phases.
        session.setAttribute(NettyChannel.ATTR_KEY_CLIENTID, msg.getClientID());

        //TODO: 暂时不用，会引起bug: https://github.com/andsel/moquette/issues/102
        //目前心跳策略为，服务端和客户端先协商好，而不进行动态协商。
//        session.setIdleTime(Math.round(keepAlive * 1.5f));

        ConnAckMessage okResp = new ConnAckMessage();
        okResp.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);

        session.write(okResp);

        m_sessionsStore.addNewSubscription(Subscription.createEmptySubscription(msg.getClientID(), true), msg.getClientID()); //null means EmptySubscription
        LOG.info("Connected client ID [{}] with clean session {}", msg.getClientID(), msg.isCleanSession());

    }

    /**
     * 异步处理多终端登录的策略。1:kick,2:prevent
     *
     * @param clientID
     */
    private void connectIntervalAsync(final String clientID) {

        taskExecutors.submit(new Runnable() {
            @Override
            public void run() {

                if (!ClientIds.getAccountArea(clientID).equals(Constants.SYS_AREA)) {
                    int mutiClientAllowable = OnlineStateRepository.getMutiClientAllowable(clientID);
                    Set<String> oldClientIDs = OnlineStateRepository.get(clientID);
                    if (oldClientIDs.size() > 0) {
                        if (Constants.KICK == mutiClientAllowable) {
                            for (String oldClientID : oldClientIDs) {
                                //如果旧clientID和当前clientID相同，说明是客户端断开重连，不踢
                                //反之，发送互踢消息
                                if (!Objects.equals(oldClientID, clientID)) {
                                    publishForConnectConflict(oldClientID);
                                }
                                OnlineStateRepository.remove(oldClientID);
                            }

                        } else if (Constants.PREVENT == mutiClientAllowable) {
                            //如果该域下同一个账号多终端登录的策略是prevent
                            //ignore
                        }
                    }

                }

                OnlineStateRepository.put(clientID);

            }
        });
    }

    /**
     * 处理相同账号互相踢
     *
     * @param clientID
     */
    private void publishForConnectConflict(String clientID) {
        LOG.info("publishForConnectConflict for client [{}]", clientID);
        try {
            String from = ClientIds.getAccount(clientID);
            String areaAccount = ClientIds.getAccountArea(clientID);
            KickOrder kickOrder = new KickOrder(areaAccount, from, from, clientID, null);
            client.kicker().kick(kickOrder);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 异步认证token
     *
     * @param session
     * @param name
     * @param pwd
     * @param clientID
     */
    private void authAsync(final ServerChannel session, final String name, final String pwd, final String clientID) {
        //异步方式认证token
        //这里的username是业务方传过来的token。
        Future<Boolean> f = taskExecutors.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return m_authenticator.checkValid(name, pwd, clientID);
            }
        });

        f.addListener(new GenericFutureListener<Future<Boolean>>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                boolean pass = future.getNow();
                if (!pass) {
                    authFail(session);
                }
            }
        });
    }

    //mqtt 认证失败
    private final void authFail(ServerChannel session) {
        ConnAckMessage okResp = new ConnAckMessage();
        okResp.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
        session.write(okResp);
        session.close(false);
        return;
    }

    private void cleanSession(String clientID) {
//		LOG.info("cleaning old saved subscriptions for client [{}]", clientID);
        subscriptions.removeForClient(clientID);
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
    }

    private void processPublish(String clientID, String topic, QOSType qos, ByteBuffer message, boolean retain, Integer messageID) {
        LOG.info("PUBLISH from clientID [{}] on topic [{}] with QoS [{}]", clientID, topic, qos);

        forward2Subscribers(topic, qos, message, retain, messageID);
    }

    /**
     * Flood the subscribers with the message to notify. MessageID is optional and should only used for QoS 1 and 2
     */
    void forward2Subscribers(String topic, AbstractMessage.QOSType pubQos, ByteBuffer origMessage,
                             boolean retain, Integer messageID) {
        LOG.debug("forward2Subscribers republishing to existing subscribers that matches the topic {}", topic);

        List<Subscription> matched = subscriptions.matches(topic);

        for (final Subscription sub : matched) {
            AbstractMessage.QOSType qos = pubQos;
            if (qos.ordinal() > sub.getRequestedQos().ordinal()) {
                qos = sub.getRequestedQos();
            }

            LOG.debug("Broker republishing to client [{}] topic [{}] qos [{}], active {}",
                    sub.getClientId(), sub.getTopicFilter(), qos, sub.isActive());
            ByteBuffer message = origMessage.duplicate();
            sendPublish(sub.getClientId(), topic, qos, message, false, null);

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
            throw new RuntimeException("Internal bad error, qos must be 0");
        }

        if (m_clientIDs == null) {
            throw new RuntimeException("Internal bad error, found m_clientIDs to null while it should be initialized, somewhere it's overwritten!!");
        }
//		LOG.debug("clientIDs are {}", m_clientIDs);
        if (m_clientIDs.get(clientId) == null) {
            //重新清理异常的订阅者
            reclean(clientId);
            LOG.error("Can't find a ConnectionDescriptor for client [{}] in cache", clientId);
            throw new RuntimeException(String.format("Can't find a ConnectionDescriptor for client <%s> in cache", clientId));
        }
        LOG.debug("Session for clientId {} is {}", clientId, m_clientIDs.get(clientId).getSession());
//        m_clientIDs.get(clientId).getSession().write(pubMessage);

        publishToMainDisruptor(new OutputMessagingEvent(m_clientIDs.get(clientId).getSession(), pubMessage));
    }


    @MQTTMessage(message = DisconnectMessage.class)
    void processDisconnect(final ServerChannel session, DisconnectMessage msg) throws InterruptedException {
        final String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);

        m_clientIDs.remove(clientID);

        cleanAll(clientID);

        session.close(true);

        LOG.info("DISCONNECT client [{}]", clientID);

    }

    //清理订阅资源和redis资源
    private void cleanAll(String clientID) {
        final Set<Subscription> subs = m_sessionsStore.getSubscriptionById(clientID);
        Set<Subscription> cloned = copy(subs);

        cleanSession(clientID);

        if (cloned != null) {
            cleanRedisAsync(clientID, cloned);
        }
    }

    /**
     * 异步清理redis
     *
     * @param clientID
     * @param subs
     */
    private void cleanRedisAsync(final String clientID, final Set<Subscription> subs) {
        taskExecutors.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Subscription s : subs) {
                        TopicRouterRepository.clean(s.getTopicFilter());
                    }
                    //clear onlineState
                    OnlineStateRepository.remove(clientID);
                } catch (Exception e) {
                    LOG.error("cleanRedisAsync failed with clientID [{}]", clientID);
                    LOG.error(e.getMessage(), e);
                }

            }
        });
    }


    /**
     * 处理客户端连接丢失
     * 两种情况：1.客户端由于网络不稳定而造成的连接丢失
     * 2.由于使用同一个clientID进行connect操作,服务端主动断开客户端连接
     * <p/>
     * 当发生情况2时flag必定为true
     *
     * @param evt
     */
    void processConnectionLost(final LostConnectionEvent evt) {

        if (!flag) {

            String clientID = evt.clientID;
            //If already removed a disconnect message was already processed for this clientID
            if (m_clientIDs.remove(clientID) != null) {

                cleanAll(clientID);

            }
            LOG.info("Lost connection with client [{}]", clientID);
        }
        //重置flag标识
        flag = false;

    }

    private Set<Subscription> copy(Set<Subscription> subscription) {
        if (subscription.size() > 0) {
            return Sets.newHashSet(subscription);
        }
        return null;
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
        }
        //ack the client
        UnsubAckMessage ackMessage = new UnsubAckMessage();
        ackMessage.setMessageID(messageID);

        unSubscribeAsync(session, msg, ackMessage);
    }

    /**
     * 异步从redis清理topic，并且进行ack
     *
     * @param session
     * @param msg
     * @param ackMessage
     */
    private void unSubscribeAsync(final ServerChannel session, final UnsubscribeMessage msg, final UnsubAckMessage ackMessage) {
        Future<Boolean> f = taskExecutors.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                for (String topic : msg.topicFilters()) {
                    TopicRouterRepository.clean(topic);
                }
                return true;
            }
        });

        f.addListener(new GenericFutureListener<Future<Boolean>>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                boolean result = future.getNow();
                if (result) {
                    session.write(ackMessage);
                }
            }
        });
    }

    @MQTTMessage(message = SubscribeMessage.class)
    void processSubscribe(final ServerChannel session, final SubscribeMessage msg) {
        String clientID = (String) session.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
        boolean cleanSession = (Boolean) session.getAttribute(NettyChannel.ATTR_KEY_CLEANSESSION);
        LOG.debug("SUBSCRIBE client [{}] packetID {}", clientID, msg.getMessageID());

        //ack the client
        final SubAckMessage ackMessage = new SubAckMessage();
        ackMessage.setMessageID(msg.getMessageID());

        //reply with requested qos
        for (SubscribeMessage.Couple req : msg.subscriptions()) {
            AbstractMessage.QOSType qos = AbstractMessage.QOSType.values()[req.getQos()];
            String topicFilter = req.getTopicFilter();
            Subscription newSubscription = new Subscription(clientID, topicFilter, qos, cleanSession);

            m_sessionsStore.addNewSubscription(newSubscription, clientID);
            subscriptions.add(newSubscription);

            ackMessage.addType(qos);
        }

        subscribeAsync(session, msg, ackMessage);

    }

    /**
     * 异步添加topic到redis中，并且进行ack
     *
     * @param session
     * @param msg
     * @param ackMessage
     */
    private void subscribeAsync(final ServerChannel session, final SubscribeMessage msg, final SubAckMessage ackMessage) {
        Future<Boolean> f = taskExecutors.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                for (SubscribeMessage.Couple req : msg.subscriptions()) {
                    TopicRouterRepository.add(req.getTopicFilter());
                }
                return true;
            }
        });

        f.addListener(new GenericFutureListener<Future<Boolean>>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                boolean result = future.getNow();
                if (result) {
                    session.write(ackMessage);
                }
            }
        });
    }

    public void publishToMainDisruptor(MessagingEvent msgEvent) {
        LOG.debug("publishToMainDisruptor publishing event on output {}", msgEvent);
        long sequence = mainRingBuffer.next();
        ValueEvent event = mainRingBuffer.get(sequence);

        event.setEvent(msgEvent);

        mainRingBuffer.publish(sequence);
    }


    public void onEvent(ValueEvent t, long l, boolean bln) throws Exception {

        try {
            MessagingEvent evt = t.getEvent();
            //always OutputMessagingEvent instance
            OutputMessagingEvent outEvent = (OutputMessagingEvent) evt;
            LOG.debug("Output event, sending {}", outEvent.getMessage());
            outEvent.getChannel().write(outEvent.getMessage());
        } finally {
            t.setEvent(null);
        }

    }

    /**
     * 清理已经断开连接的client资源
     *
     * @param clientID
     */
    private void reclean(String clientID) {

        try {
            cleanAll(clientID);
        } catch (Exception e) {
            LOG.error("reclean error");
            LOG.error(e.getMessage(), e);
        }

    }

}
