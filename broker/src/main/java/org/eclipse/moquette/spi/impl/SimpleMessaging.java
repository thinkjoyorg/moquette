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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
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
	private RingBuffer<ValueEvent> m_ringBuffer, io_ringBuffer;
	private IMessagesStore m_storageService;
	private ISessionsStore m_sessionsStore;
	private ExecutorService m_executor, io_executor;
	private Disruptor<ValueEvent> m_disruptor, io_disruptor;
	private boolean benchmarkEnabled = false;

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
		m_executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("MessagingDispatcher"));
		io_executor = Executors.newFixedThreadPool(Constants.wThreads + 1, new DefaultThreadFactory("IoMessagingDispatcher"));
		m_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, m_executor, ProducerType.SINGLE, new BlockingWaitStrategy());
		io_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, Constants.SIZE_RINGBUFFER, io_executor, ProducerType.SINGLE, new BlockingWaitStrategy());
		/*Disruptor<ValueEvent> m_disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, 1024 * 32, m_executor,
	            ProducerType.MULTI, new BusySpinWaitStrategy());*/
		m_disruptor.handleEventsWith(this);
		m_disruptor.handleExceptionsWith(new DisruptorExceptionHandler());
		m_disruptor.start();
		WorkHandler[] handlers = create();
		io_disruptor.handleEventsWithWorkerPool(handlers);
		io_disruptor.handleExceptionsWith(new DisruptorExceptionHandler());
		io_disruptor.start();

		// Get the ring buffer from the Disruptor to be used for publishing.
		m_ringBuffer = m_disruptor.getRingBuffer();
		io_ringBuffer = io_disruptor.getRingBuffer();

		annotationSupport.processAnnotations(m_processor);
		processInit(configProps);
//        disruptorPublish(new InitEvent(configProps));
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

	private void publishToIoDisruptor(MessagingEvent msgEvent) {
		LOG.debug("publishToIoDisruptor publishing event {}", msgEvent);
		long sequence = io_ringBuffer.next();
		ValueEvent event = io_ringBuffer.get(sequence);

		event.setEvent(msgEvent);

		io_ringBuffer.publish(sequence);

	}

	@Override
	public void lostConnection(String clientID) {
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
		if (msg.getMessageType() == AbstractMessage.CONNECT || msg.getMessageType() == AbstractMessage.SUBSCRIBE) {
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
			LOG.debug("LostConnection:[{}]", ((LostConnectionEvent) evt).clientID);
			LostConnectionEvent lostEvt = (LostConnectionEvent) evt;
			m_processor.processConnectionLost(lostEvt);
			return;
		}

		if (evt instanceof ProtocolEvent) {
			ServerChannel session = ((ProtocolEvent) evt).getSession();
			AbstractMessage message = ((ProtocolEvent) evt).getMessage();
			try {
				if (benchmarkEnabled) {
					++count;
					long startTime = System.nanoTime();
					annotationSupport.dispatch(session, message);
					delay += System.nanoTime() - startTime;
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
