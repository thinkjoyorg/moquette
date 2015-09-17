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

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.server.IAuthenticator;
import org.eclipse.moquette.server.ServerChannel;
import org.eclipse.moquette.spi.IMessagesStore;
import org.eclipse.moquette.spi.IMessaging;
import org.eclipse.moquette.spi.ISessionsStore;
import org.eclipse.moquette.spi.impl.events.LostConnectionEvent;
import org.eclipse.moquette.spi.impl.events.MessagingEvent;
import org.eclipse.moquette.spi.impl.events.ProtocolEvent;
import org.eclipse.moquette.spi.impl.events.StopEvent;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;
import org.eclipse.moquette.spi.impl.thinkjoy.AreaAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * Singleton class that orchestrate the execution of the protocol.
 *
 * Uses the LMAX Disruptor to serialize the incoming, requests, because it work in a evented fashion;
 * the requests income from front Netty connectors and are dispatched to the
 * ProtocolProcessor.
 *
 * @author andrea
 */

public class SimpleMessaging implements IMessaging, EventHandler<ValueEvent> {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleMessaging.class);
	private static SimpleMessaging INSTANCE;
	private final ProtocolProcessor m_processor = new ProtocolProcessor();
	private final AnnotationSupport annotationSupport = new AnnotationSupport();
	CountDownLatch m_stopLatch;
	private SubscriptionsStore subscriptions;

	private boolean benchmarkEnabled = false;
	private IMessagesStore m_storageService;
	private ISessionsStore m_sessionsStore;
	/**
	 * 将事件分离，用合适的模型处理合适的事件，是提高整体吞吐量的关键。
	 */

    private Disruptor<ValueEvent> m_disruptor;// publish，subscribe,unsubscribe 事件分发器,单线程模型
//			io_disruptor;// connect 事件分发器， 多线程模型
    /**
	 * lostConn_disruptor; //lost connection 事件分发器，单线程模型。涉及到资源竞争，这里最好使用单线程处理
	 */

    private RingBuffer<ValueEvent> m_ringBuffer;
//			io_ringBuffer;
    /**
	 * lostConn_ringBuffer;
	 */

    private ExecutorService m_executor;
//			io_executor;

	/**
	 * lostConn_executor;
	 */

	private SimpleMessaging() {
	}

	public static SimpleMessaging getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new SimpleMessaging();
		}
		return INSTANCE;
	}

	public void init(Properties configProps) {
		subscriptions = new SubscriptionsStore();

		initDisruptor();
		startDisruptor();
		initRingBuffer();

		annotationSupport.processAnnotations(m_processor);

		processInit(configProps);
	}

	private void initRingBuffer() {
		m_ringBuffer = m_disruptor.getRingBuffer();
	}

	private void startDisruptor() {
		m_disruptor.handleEventsWith(this);
		m_disruptor.start();

	}

	private void initDisruptor() {
		m_executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("MessagingDispatcher"));
		m_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, m_executor);
	}
	private void disruptorPublish(MessagingEvent msgEvent) {
		LOG.debug("disruptorPublish publishing event {}", msgEvent);
		long sequence = m_ringBuffer.next();
		ValueEvent event = m_ringBuffer.get(sequence);

		event.setEvent(msgEvent);

		m_ringBuffer.publish(sequence);
	}



	@Override
	public void lostConnection(String clientID) {
		disruptorPublish(new LostConnectionEvent(clientID));
	}

	@Override
	public void handleProtocolMessage(ServerChannel session, AbstractMessage msg) {
        disruptorPublish(new ProtocolEvent(session, msg));
    }

	@Override
	public void stop() {
		m_stopLatch = new CountDownLatch(1);
		disruptorPublish(new StopEvent());
		boolean elapsed = false;
		try {
			//wait the callback notification from the protocol processor thread
			LOG.debug("waiting 30 sec to m_stopLatch");
			elapsed = !m_stopLatch.await(30, TimeUnit.SECONDS);

		} catch (InterruptedException ex) {
			LOG.error(null, ex);
		}
		LOG.debug("after m_stopLatch");
		this.destory();
		m_processor.destory();
		if (elapsed) {
			LOG.error("Can't stop the server in 30 seconds");
		}
	}

	private void destory() {
		m_executor.shutdown();
		m_disruptor.shutdown();
    }

	@Override
	public void onEvent(ValueEvent t, long l, boolean bln) throws Exception {
		MessagingEvent evt = t.getEvent();
		/**
		 * avoid memory leak
		 */
		t.setEvent(null);

		LOG.debug("onEvent processing messaging event from input ringbuffer {}", evt);
		if (evt instanceof StopEvent) {
			processStop();
			return;
		}
		if (evt instanceof LostConnectionEvent) {
			LostConnectionEvent lostEvt = (LostConnectionEvent) evt;
			m_processor.processConnectionLost(lostEvt);
			return;
		}

		if (evt instanceof ProtocolEvent) {
			ServerChannel session = ((ProtocolEvent) evt).getSession();
			AbstractMessage message = ((ProtocolEvent) evt).getMessage();
			try {
                annotationSupport.dispatch(session, message);
            } catch (Throwable th) {
				LOG.error("Serious error processing the message {} for {}", message, th);
			}
		}

        if (benchmarkEnabled) {
            printDisruptorBacklog(l);
        }

	}

    /**
     * 当积压的事件超过100(可配置)个时，打印出积压的事件数量，起到报警作用
     *
     * @param l
     */
    private void printDisruptorBacklog(long l) {
        long cursor = m_disruptor.getCursor();
        long backlog = cursor - l;
        if (backlog < 0) {
            backlog = 0;
        }
        if (backlog > 100) {
            LOG.info("MessagingDisruptor also has [{}] events was un-used", backlog);
        }
    }

    private void processInit(Properties props) {
        benchmarkEnabled = Boolean.parseBoolean(System.getProperty("moquette.processor.benchmark", "false"));

		//TODO use a property to select the storage path
		//TODO 此版本不需要持久化
//	    MapDBPersistentStore mapStorage = new MapDBPersistentStore(props.getProperty(org.eclipse.moquette.commons.Constants.PERSISTENT_STORE_PROPERTY_NAME, ""));
		MemoryStorageService mapStorage = new MemoryStorageService();
		m_storageService = mapStorage;
		m_sessionsStore = mapStorage;

		m_storageService.initStore();

		subscriptions.init(m_sessionsStore);

		IAuthenticator authenticator = new AreaAuthenticator();

		m_processor.init(subscriptions, m_storageService, m_sessionsStore, authenticator);
	}


	private void processStop() {
		LOG.debug("processStop invoked");
		m_storageService.close();

		subscriptions = null;
		m_stopLatch.countDown();
	}
}
