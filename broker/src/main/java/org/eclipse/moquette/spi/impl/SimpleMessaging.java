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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
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

/**
 * 将事件分为两类进行分发：一类是带有io操作的事件(connect, subscribe)，一类不带io操作的事件(publish)。
 * 带有io操作的事件，被多线程分发器(io_disruptor)所分发
 * 不带io操作的事件，被单线程分发器(m_disruptor)所分发。
 */
public class SimpleMessaging implements IMessaging, EventHandler<ValueEvent> {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleMessaging.class);
	private static SimpleMessaging INSTANCE;
	private final ProtocolProcessor m_processor = new ProtocolProcessor();
	private final AnnotationSupport annotationSupport = new AnnotationSupport();
	private final String formatString = "%d | %f |  %d \n";
	CountDownLatch m_stopLatch;
	long delay = 0L;
	long count = 0L;
	private SubscriptionsStore subscriptions;

	private boolean benchmarkEnabled = false;
	private IMessagesStore m_storageService;
	private ISessionsStore m_sessionsStore;
	/**
	 * 将事件分离，用合适的模型处理合适的事件，是提高整体吞吐量的关键。
	 */

	private Disruptor<ValueEvent> m_disruptor,// publish，subscribe,unsubscribe 事件分发器,单线程模型
			io_disruptor;// connect 事件分发器， 多线程模型
	/**
	 * lostConn_disruptor; //lost connection 事件分发器，单线程模型。涉及到资源竞争，这里最好使用单线程处理
	 */

	private RingBuffer<ValueEvent> m_ringBuffer,
			io_ringBuffer;
	/**
	 * lostConn_ringBuffer;
	 */

	private ExecutorService m_executor,
			io_executor;

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
		io_ringBuffer = io_disruptor.getRingBuffer();
		//lostConn_ringBuffer = lostConn_disruptor.getRingBuffer();
	}

	private void startDisruptor() {
		m_disruptor.handleEventsWith(this);
		m_disruptor.start();

		io_disruptor.handleEventsWithWorkerPool(create());
		io_disruptor.start();

		//lostConn_disruptor.handleEventsWith(new LostConnProcessor());
		//lostConn_disruptor.start();
	}

	private void initDisruptor() {
		m_executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("MessagingDispatcher"));
//		lostConn_executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("LostConnMessagingDispatcher"));
		io_executor = Executors.newFixedThreadPool(Constants.wThreads + 1, new DefaultThreadFactory("IoMessagingDispatcher"));

		m_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, m_executor);
		io_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, io_executor);
//		lostConn_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, lostConn_executor);

	}

	private final WorkHandler[] create() {
		int size = Constants.wThreads + 1;
		WorkHandler[] handlers = new WorkHandler[size];
		for (int i = 0; i < handlers.length; i++) {
			handlers[i] = new IoMessagingProcessor();
		}
		return handlers;
	}

	private void disruptorPublish(MessagingEvent msgEvent) {
		LOG.debug("disruptorPublish publishing event {}", msgEvent);
		long sequence = m_ringBuffer.next();
		ValueEvent event = m_ringBuffer.get(sequence);

		event.setEvent(msgEvent);

		m_ringBuffer.publish(sequence);
	}

//	private void publishToLostConnDisruptor(MessagingEvent msgEvent) {
//		LOG.debug("publishToLostConnDisruptor publishing event {}", msgEvent);
//		long sequence = lostConn_ringBuffer.next();
//		ValueEvent event = lostConn_ringBuffer.get(sequence);
//
//		event.setEvent(msgEvent);
//
//		lostConn_ringBuffer.publish(sequence);
//	}

	private void publishToIoDisruptor(MessagingEvent msgEvent) {
		LOG.debug("publishToIoDisruptor publishing event {}", msgEvent);
		long sequence = io_ringBuffer.next();
		ValueEvent event = io_ringBuffer.get(sequence);

		event.setEvent(msgEvent);

		io_ringBuffer.publish(sequence);

	}

	@Override
	public void lostConnection(String clientID) {
//		publishToLostConnDisruptor(new LostConnExEvent(clientID,m_processor));
		disruptorPublish(new LostConnectionEvent(clientID));
	}

	@Override
	public void handleProtocolMessage(ServerChannel session, AbstractMessage msg) {
		if (hasIoLogic(msg)) {
			publishToIoDisruptor(new ProtocolExEvent(session, msg, annotationSupport));
		} else {
			disruptorPublish(new ProtocolEvent(session, msg));
		}
	}

	/**
	 * has io logic,  usually need thread pool .
	 *
	 * @param msg
	 * @return
	 */
	private final boolean hasIoLogic(AbstractMessage msg) {
		if (msg.getMessageType() == AbstractMessage.CONNECT/** || msg.getMessageType() == AbstractMessage.SUBSCRIBE*/) {
			return true;
		} else {
			return false;
		}
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
		io_executor.shutdown();
		m_disruptor.shutdown();
		io_disruptor.shutdown();
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
			long start = System.nanoTime();
			m_processor.processConnectionLost(lostEvt);
			long deley = System.nanoTime() - start;
			LOG.info("processed lost connection takes [{}] ms", deley / 1000000);
			return;
		}

		if (evt instanceof ProtocolEvent) {
			ServerChannel session = ((ProtocolEvent) evt).getSession();
			AbstractMessage message = ((ProtocolEvent) evt).getMessage();
			try {
				if (benchmarkEnabled && message.getMessageType() == AbstractMessage.PUBLISH) {
					++count;
					long startTime = System.nanoTime();
					annotationSupport.dispatch(session, message);
					long l2 = System.nanoTime() - startTime;
					delay += l2;
					LOG.info("processed type [{}] msg takes [{}] ms", message.getMessageType(), l2 / 1000000);
					if (count % 10 == 0) {
						long p = m_ringBuffer.getCursor();
						long backlog = Utils.count(p, l);
						long l1 = delay / 1000000;
						double r = 0.0;
						if (l1 == 0) {
							r = Double.POSITIVE_INFINITY;
						} else {
							r = count / l1;
						}
						System.out.format(formatString, count, r, backlog);
					}
				} else {
					annotationSupport.dispatch(session, message);
				}

			} catch (Throwable th) {
				LOG.error("Serious error processing the message {} for {}", message, th);
			}
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

		//List<Subscription> storedSubscriptions = m_sessionsStore.listAllSubscriptions();
		//subscriptions.init(storedSubscriptions);
		subscriptions.init(m_sessionsStore);

//	    String passwdPath = props.getProperty(org.eclipse.moquette.commons.Constants.PASSWORD_FILE_PROPERTY_NAME, "");
//	    String configPath = System.getProperty("moquette.path", null);
//	    IAuthenticator authenticator;
//        if (passwdPath.isEmpty()) {
//            authenticator = new AcceptAllAuthenticator();
//        } else {
//            authenticator = new FileAuthenticator(configPath, passwdPath);
//        }
		IAuthenticator authenticator = new AreaAuthenticator();

		m_processor.init(subscriptions, m_storageService, m_sessionsStore, authenticator);
	}


	private void processStop() {
		LOG.debug("processStop invoked");
		m_storageService.close();
//		LOG.debug("subscription tree {}", subscriptions.dumpTree());
//        m_eventProcessor.halt();
//        m_executor.shutdown();

		subscriptions = null;
		m_stopLatch.countDown();

//		if (benchmarkEnabled) {
//			//log metrics
//			histogram.outputPercentileDistribution(System.out, 1000.0);
//		}
	}
}
