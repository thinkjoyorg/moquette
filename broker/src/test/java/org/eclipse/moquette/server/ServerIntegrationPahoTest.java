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
package org.eclipse.moquette.server;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import cn.thinkjoy.cloudstack.cache.RedisRepository;
import cn.thinkjoy.cloudstack.cache.RedisRepositoryFactory;
import cn.thinkjoy.im.common.ClientIds;
import cn.thinkjoy.im.common.IMConfig;
import cn.thinkjoy.im.common.PlatformType;
import com.google.common.collect.Lists;
import org.eclipse.moquette.spi.impl.thinkjoy.TopicRouterRepository;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class ServerIntegrationPahoTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationPahoTest.class);

    static MqttClientPersistence s_dataStore;
    static MqttClientPersistence s_pubDataStore;
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	Server m_server;
	IMqttClient m_client;
	TestCallback m_callback;
	RedisRepository<String, String> jedis;

	@BeforeClass
	public static void beforeTests() {
		String tmpDir = System.getProperty("java.io.tmpdir");
		s_dataStore = new MqttDefaultFilePersistence(tmpDir);
		s_pubDataStore = new MqttDefaultFilePersistence(tmpDir + File.separator + "publisher");
	}

    protected void startServer() throws IOException {
        m_server = new Server();
	    m_server.startServer(new Properties());
    }

    @Before
    public void setUp() throws Exception {
	    File dbFile = new File(org.eclipse.moquette.commons.Constants.DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME);
	    assertFalse(String.format("The DB storagefile %s already exists", org.eclipse.moquette.commons.Constants.DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME), dbFile.exists());

	    jedis = RedisRepositoryFactory.getRepository(IMConfig.CACHE_TOPIC_ROUTING_TABLE.get());
	    startServer();

	    String clientID = ClientIds.generateClientId("zl", "gbdai", PlatformType.Android);

	    m_client = new MqttClient("tcp://localhost:1883", clientID, s_dataStore);
	    m_callback = new TestCallback();
	    m_client.setCallback(m_callback);
    }

    @After
    public void tearDown() throws Exception {
	    if (m_client.isConnected()) {
		    m_client.disconnect();
	    }

	    m_server.stopServer();
	    File dbFile = new File(m_server.getProperties().getProperty(org.eclipse.moquette.commons.Constants.PERSISTENT_STORE_PROPERTY_NAME));
	    if (dbFile.exists()) {
		    dbFile.delete();
	    }
	    assertFalse(dbFile.exists());
    }

	@Test
	public void testAddRouteAfterSubscribe() throws Exception {
//		LOG.info("*** testAddRouteAfterSubscribe ***");
//		m_client.connect();
//		m_client.subscribe("/topic", 0);
//		m_client.subscribe("/group", 0);
//		m_client.subscribe("/ids", 0);
//		Thread.sleep(5000);
//		assertEquals(1, jedis.keys("/topic").size());
//		assertEquals(1, jedis.keys("/group").size());
//		assertEquals(1, jedis.keys("/ids").size());
//		//assertEquals("tcp://0.0.0.0:1883", jedis.sMembers("/topic").iterator().next());


		System.out.println("s");

	}

	@Test
	public void testCleanRouteAfterUnsubscribe() throws Exception {
		LOG.info("*** testCleanRouteAfterUnsubscribe ***");
		m_client.connect();
		m_client.subscribe("/topic", 0);
		m_client.subscribe("/topic1", 0);
		m_client.subscribe("/topic2", 0);

		m_client.unsubscribe("/topic");
		assertEquals(0, jedis.sMembers("/topic").size());
		assertEquals(1, jedis.sMembers("/topic1").size());
		assertEquals(1, jedis.sMembers("/topic2").size());
	}

	@Test
	public void testCleanRouteAfterDisconnect() throws Exception {
		LOG.info("*** testCleanRouteAfterDisconnect ***");
		m_client.connect();
		m_client.subscribe("/t1", 0);
		m_client.subscribe("/t2", 0);
		m_client.subscribe("/t3", 0);
		m_client.disconnect();
		Thread.sleep(1000);
		assertEquals(0, jedis.sMembers("/t1").size());
		assertEquals(0, jedis.sMembers("/t2").size());
		assertEquals(0, jedis.sMembers("/t3").size());
	}

	@Test
	public void testSubscribe() throws Exception {
		LOG.info("*** testSubscribe ***");
		MqttConnectOptions options = new MqttConnectOptions();
		options.setUserName("00000000");
		options.setPassword("111111".toCharArray());


		m_client.connect(options);
		m_client.subscribe("/topic");
//		m_client.publish();
//		m_client.subscribe(null, 0);

        MqttMessage message = new MqttMessage("Hello world!!".getBytes());
        message.setQos(0);
        message.setRetained(false);
        m_client.publish("/topic", message);

		assertEquals("/topic", m_callback.getTopic());
		assertEquals("Hello world!!", m_callback.getMessage(false).toString());
	}

	@Test
	public void testCleanSession_maintainClientSubscriptions() throws Exception {
		LOG.info("*** testCleanSession_maintainClientSubscriptions ***");
		MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.disconnect();

        //reconnect and publish
        MqttClient anotherClient = new MqttClient("tcp://localhost:1883", "TestClient", s_dataStore);
        m_callback = new TestCallback();
        anotherClient.setCallback(m_callback);
        anotherClient.connect(options);
        anotherClient.publish("/topic", "Test my payload".getBytes(), 0, false);

        assertEquals("/topic", m_callback.getTopic());
    }

    @Test
    public void testCleanSession_maintainClientSubscriptions_againstClientDestruction() throws Exception {
        LOG.info("*** testCleanSession_maintainClientSubscriptions_againstClientDestruction ***");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.disconnect();

        //reconnect and publish
        m_client.connect(options);
        m_client.publish("/topic", "Test my payload".getBytes(), 0, false);

        assertEquals("/topic", m_callback.getTopic());
    }

    /**
     * Check that after a client has connected with clean session false, subscribed
     * to some topic and exited, if it reconnect with clean session true, the m_server
     * correctly cleanup every previous subscription
     */
    @Test
    public void testCleanSession_correctlyClientSubscriptions() throws Exception {
        LOG.info("*** testCleanSession_correctlyClientSubscriptions ***");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.disconnect();

        //reconnect and publish
        m_client.connect();
        m_client.publish("/topic", "Test my payload".getBytes(), 0, false);

        assertNull(m_callback.getMessage(false));
    }

    @Test
    public void testCleanSession_maintainClientSubscriptions_withServerRestart() throws Exception {
        LOG.info("*** testCleanSession_maintainClientSubscriptions_withServerRestart ***");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.disconnect();

        m_server.stopServer();

        m_server.startServer();

        //reconnect and publish
        m_client.connect(options);
        m_client.publish("/topic", "Test my payload".getBytes(), 0, false);

        assertEquals("/topic", m_callback.getTopic());
    }

    @Test
    public void testRetain_maintainMessage_againstClientDestruction() throws Exception {
        LOG.info("*** testRetain_maintainMessage_againstClientDestruction ***");
        m_client.connect();
        m_client.publish("/topic", "Test my payload".getBytes(), 1, true);
        m_client.disconnect();

        //reconnect and publish
        m_client.connect();
        m_client.subscribe("/topic", 0);

        assertEquals("/topic", m_callback.getTopic());
    }

    @Test
    public void testUnsubscribe_do_not_notify_anymore_same_session() throws Exception {
        LOG.info("*** testUnsubscribe_do_not_notify_anymore_same_session ***");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.publish("/topic", "Test my payload".getBytes(), 0, false);
        //m_client.disconnect();
        assertEquals("/topic", m_callback.getTopic());

        m_client.unsubscribe("/topic");
        m_callback.reinit();
        m_client.publish("/topic", "Test my payload".getBytes(), 0, false);

        assertNull(m_callback.getMessage(false));
    }

    @Test
    public void testUnsubscribe_do_not_notify_anymore_new_session() throws Exception {
        LOG.info("*** testUnsubscribe_do_not_notify_anymore_new_session ***");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.publish("/topic", "Test my payload".getBytes(), 0, false);
        //m_client.disconnect();
        assertEquals("/topic", m_callback.getTopic());

        m_client.unsubscribe("/topic");
        m_client.disconnect();

        m_callback.reinit();
        m_client.connect(options);
        m_client.publish("/topic", "Test my payload".getBytes(), 0, false);

        assertNull(m_callback.getMessage(false));
    }

    @Test
    public void testPublishWithQoS1() throws Exception {
        LOG.info("*** testPublishWithQoS1 ***");
        m_client.connect();
	    m_client.subscribe("/topic/#", 1);
	    m_client.publish("/topic", "Hello MQTT".getBytes(), 1, false);
	    m_client.disconnect();

        //reconnect and publish
        MqttMessage message = m_callback.getMessage(true);
	    assertEquals("Hello MQTT", message.toString());
	    assertEquals(1, message.getQos());
    }

	@Test
	public void testPublishWithQoS1_notCleanSession() throws Exception {
		LOG.info("*** testPublishWithQoS1_notCleanSession ***");
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCleanSession(false);
		m_client.connect(options);
		m_client.subscribe("/topic", 1);
        m_client.disconnect();

        //publish a QoS 1 message another client publish a message on the topic
        publishFromAnotherClient("/topic", "Hello MQTT".getBytes(), 1);

        m_client.connect(options);

        assertEquals("Hello MQTT", m_callback.getMessage(true).toString());
    }

	@Test
	public void checkReceivePublishedMessage_after_a_reconnect_with_notCleanSession() throws Exception {
		LOG.info("*** checkReceivePublishedMessage_after_a_reconnect_with_notCleanSession ***");
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCleanSession(false);
		m_client.connect(options);
		m_client.subscribe("/topic", 1);
        m_client.disconnect();

		m_client.connect(options);
		m_client.subscribe("/topic", 1);

		//publish a QoS 1 message another client publish a message on the topic
		publishFromAnotherClient("/topic", "Hello MQTT".getBytes(), 1);

		//Verify that after a reconnection the client receive the message
		MqttMessage message = m_callback.getMessage(true);
		assertNotNull(message);
		assertEquals("Hello MQTT", message.toString());
    }

	private void publishFromAnotherClient(String topic, byte[] payload, int qos) throws Exception {
		IMqttClient anotherClient = new MqttClient("tcp://localhost:1883", "TestClientPUB", s_pubDataStore);
		anotherClient.connect();
		anotherClient.publish(topic, payload, qos, false);
		anotherClient.disconnect();
	}

    @Test
    public void testPublishWithQoS2() throws Exception {
        LOG.info("*** testPublishWithQoS2 ***");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 2);
        m_client.disconnect();

        //publish a QoS 1 message another client publish a message on the topic
        publishFromAnotherClient("/topic", "Hello MQTT".getBytes(), 2);
        m_callback.reinit();
        m_client.connect(options);

	    MqttMessage message = m_callback.getMessage(true);
	    assertEquals("Hello MQTT", message.toString());
	    assertEquals(2, message.getQos());
    }

	@Test
	public void testPublishReceiveWithQoS2() throws Exception {
		LOG.info("*** testPublishReceiveWithQoS2 ***");
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCleanSession(false);
		m_client.connect(options);
		m_client.subscribe("/topic", 2);
        m_client.disconnect();

        //publish a QoS 2 message another client publish a message on the topic
        publishFromAnotherClient("/topic", "Hello MQTT".getBytes(), 2);
        m_callback.reinit();
        m_client.connect(options);

		assertNotNull(m_callback);
		MqttMessage message = m_callback.getMessage(true);
		assertNotNull(message);
		assertEquals("Hello MQTT", message.toString());
	}

	@Test
	public void avoidMultipleNotificationsAfterMultipleReconnection_cleanSessionFalseQoS1() throws Exception {
		LOG.info("*** avoidMultipleNotificationsAfterMultipleReconnection_cleanSessionFalseQoS1, issue #16 ***");
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCleanSession(false);
		m_client.connect(options);
		m_client.subscribe("/topic", 1);
        m_client.disconnect();

		publishFromAnotherClient("/topic", "Hello MQTT 1".getBytes(), 1);
		m_callback.reinit();
		m_client.connect(options);

		assertNotNull(m_callback);
		MqttMessage message = m_callback.getMessage(true);
		assertNotNull(message);
		assertEquals("Hello MQTT 1", message.toString());
		m_client.disconnect();

		//publish other message
		publishFromAnotherClient("/topic", "Hello MQTT 2".getBytes(), 1);

		//reconnect the second time
		m_callback.reinit();
		m_client.connect(options);
		assertNotNull(m_callback);
		message = m_callback.getMessage(true);
		assertNotNull(message);
		assertEquals("Hello MQTT 2", message.toString());
    }

	@Test
	public void testOnlineState() throws Exception {
//		LOG.info("*** testOnlineState ***");
//		MqttConnectOptions options = new MqttConnectOptions();
//		options.setCleanSession(true);
//		String clientId = ClientIds.generateClientId("zl", "gbdai", PlatformType.Android());
//		MqttClient client = new MqttClient("tcp://localhost:1883", clientId);
//		client.connect(options);
//		String accountArea = ClientIds.getAccountArea(clientId);
//		String account = ClientIds.getAccount(clientId);
//		String key = accountArea.concat(account);
//
//		assertTrue(jedis.sismember(key, clientId));
//
//		client.disconnect();
//		Set<String> smembers = jedis.smembers(key);
//		if(smembers.size() > 0){
//			assertFalse(jedis.sismember(key,clientId));
//		}
	}

	@Test
	public void testOnlineStateManager() throws Exception {
//		String clientId1 = ClientIds.generateClientId("zl", "gbdai", PlatformType.Android());
//		String clientId2 = ClientIds.generateClientId("zl", "gbdai", PlatformType.Android());
//		String clientId3 = ClientIds.generateClientId("zl", "gbdai", PlatformType.Android());
//		OnlineStateManager.put(clientId1);
//		OnlineStateManager.put(clientId2);
//		OnlineStateManager.put(clientId3);
//		assertTrue(OnlineStateManager.get(clientId1).size() == 3);
//
//		OnlineStateManager.remove(clientId1);
//		OnlineStateManager.remove(clientId2);
//		OnlineStateManager.remove(clientId3);
//		assertTrue(OnlineStateManager.get(clientId1).size() == 0);
//
//		String clientId4 = ClientIds.generateClientId("zl", "xyzhang", PlatformType.Android());
//		String clientId5 = ClientIds.generateClientId("xyy", "gbdai", PlatformType.Android());
//
//		assertTrue(1 == OnlineStateManager.getMutiClientAllowable(clientId4));
//		assertTrue(2 == OnlineStateManager.getMutiClientAllowable(clientId5));
		//OnlineStateManager.

	}

	@Test
	public void testConnectConflict() throws Exception {
		String anotherClientID = ClientIds.generateClientId("zl", "gbdai", PlatformType.Android);
		MqttConnectOptions ops = new MqttConnectOptions();
		ops.setUserName("00000000");
		ops.setPassword("111111".toCharArray());
		m_client.connect(ops);
		m_client.subscribe(m_client.getClientId());
		String tmpDir = System.getProperty("java.io.tmpdir");
		MqttClientPersistence anotherStore = new MqttDefaultFilePersistence(tmpDir + File.separator + "anotherClient");
		MqttClient anotherClient = new MqttClient("tcp://localhost:1883", anotherClientID, anotherStore);

		anotherClient.connect(ops);
		anotherClient.disconnect();

		//	assertTrue(Objects.equals(Constants.M_CONNECT_CONFLICT, m_callback.getMessage(true).toString()));
	}

	@Test(expected = MqttException.class)
	public void testConnectionPrevent() throws Exception {
		String anotherClientID = ClientIds.generateClientId("xyy", "xyzhang", PlatformType.Android);
		String aClientID = ClientIds.generateClientId("xyy", "xyzhang", PlatformType.Android);
		String tmpDir = System.getProperty("java.io.tmpdir");
		MqttClientPersistence anotherStore = new MqttDefaultFilePersistence(tmpDir + File.separator + "anotherClient");
		MqttClientPersistence aStore = new MqttDefaultFilePersistence(tmpDir + File.separator + "anotherClient");
		MqttClient anotherClient = new MqttClient("tcp://localhost:1883", anotherClientID, anotherStore);
		MqttClient aClient = new MqttClient("tcp://localhost:1883", aClientID, aStore);
		aClient.connect();
		anotherClient.connect();
		//	assertTrue(Objects.equals(Constants.M_CONNECT_CONFLICT, "1"));
	}

	@Test
	public void testAreaAuthicatorSUCCESS() throws Exception {
		String aClientID = ClientIds.generateClientId("zl", "xyzhang", PlatformType.Android);
		String tmpDir = System.getProperty("java.io.tmpdir");
		MqttClientPersistence aStore = new MqttDefaultFilePersistence(tmpDir + File.separator + "anotherClient");
		MqttClient aClient = new MqttClient("tcp://localhost:1883", aClientID, aStore);
		MqttConnectOptions options = new MqttConnectOptions();
		options.setConnectionTimeout(2);
		options.setUserName("00000000");
		options.setPassword("111111".toCharArray());
		aClient.connect(options);
	}

	@Test(expected = MqttException.class)
	public void testAreaAuthicatorFail() throws Exception {
		String aClientID = ClientIds.generateClientId("xyy", "xyzhang", PlatformType.Android);
		String tmpDir = System.getProperty("java.io.tmpdir");
		MqttClientPersistence aStore = new MqttDefaultFilePersistence(tmpDir + File.separator + "anotherClient");
		MqttClient aClient = new MqttClient("tcp://localhost:1883", aClientID, aStore);
		MqttConnectOptions options = new MqttConnectOptions();
		options.setUserName("xyy");
		options.setPassword("1111111".toCharArray());
		aClient.connect(options);
		System.out.printf("1");
	}

	@Test
	public void testLostConnection() throws Exception {
//		IMqttClient anotherClient = new MqttClient("tcp://localhost:1884", "TestClientPUB1", s_pubDataStore);
//		anotherClient.setCallback(m_callback);
		MqttConnectOptions options = new MqttConnectOptions();
		options.setKeepAliveInterval(30);
		m_client.connect(options);
		m_client.subscribe("/topic111");
		System.out.println("1a");
		m_client.disconnect();

//		m_client.connect(options);
//		Thread.sleep(50000);

	}


	@Test
	public void testNone() throws Exception {
//		String s = jedis.get("topicSeq^/user/xy^1");
		//System.out.printf(ClientIds.generateClientId("zl", "gbdai", ClientIds.PlatformType.Android));
//		RedisRepository<String, String> redisRepository = OnlineStateRepository.get()
//		redisRepository.set("token:00000000","00000000");
//		redisRepository
//		redisRepository.getRedisTemplate().setValueSerializer(new StringRedisSerializer());
//		redisRepository.set("token:00000000", "00000000");
//		assertEquals("00000000", redisRepository.get("token:00000000"));
//		redisRepository.set("token:00000000","00000000");


	}

	@Test
	public void testAddBatch() throws Exception {

		List<String> topics = Lists.newArrayList("t1", "t2", "t3");

		long s = System.currentTimeMillis();
//		for (int i = 0; i < 1; i++) {
//		}
		TopicRouterRepository.addBatch(topics);
		long e = System.currentTimeMillis() - s;
		System.out.println("addBatch takes : " + e + "ms");

//		MqttConnectOptions ops = new MqttConnectOptions();
//		ops.setUserName("00000000");
//		ops.setPassword("111111".toCharArray());
//		m_client.connect(ops);
//		m_client.subscribe("t1");
//		m_client.subscribe("t2");
//		m_client.subscribe("t3");

	}

	@Test
	public void testCleanBatch() throws Exception {

	}
}
