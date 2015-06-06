package org.eclipse.moquette.spi.impl;

import java.util.Set;

import cn.thinkjoy.im.api.client.IMClient;
import cn.thinkjoy.im.common.ClientIds;
import cn.thinkjoy.im.protocol.system.KickOrder;
import com.lmax.disruptor.WorkHandler;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.proto.MQTTException;
import org.eclipse.moquette.spi.impl.events.ConnectIoEvent;
import org.eclipse.moquette.spi.impl.events.ExtraIoEvent;
import org.eclipse.moquette.spi.impl.events.IoEvent;
import org.eclipse.moquette.spi.impl.events.MessagingEvent;
import org.eclipse.moquette.spi.impl.subscriptions.Subscription;
import org.eclipse.moquette.spi.impl.thinkjoy.OnlineStateRepository;
import org.eclipse.moquette.spi.impl.thinkjoy.TopicRouterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/5/29
 *
 * @version 1.0
 */

public class IoTaskProcessor implements WorkHandler<ValueEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(IoTaskProcessor.class);

	/**
	 * publish to connect conflict message kickOrder
	 */
	private final void publishForConnectConflict(String clientID, IMClient client) {
		LOG.info("publishForConnectConflict for client [{}]", clientID);
		// 等待actor就绪
		long start = System.currentTimeMillis();
		try {
			String from = ClientIds.getAccount(clientID);
			String areaAccount = ClientIds.getAccountArea(clientID);
			KickOrder kickOrder = new KickOrder(areaAccount, from, from, clientID, null);
			client.kicker().kick(kickOrder);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new MQTTException(e);
		}
		long end = System.currentTimeMillis();
		LOG.debug("publishForConnectConflict takes [{}] ms", (end - start));
	}

	@Override
	public void onEvent(ValueEvent event) throws Exception {
		try {
			MessagingEvent evt = event.getEvent();
			IoEvent ioEvent = (IoEvent) evt;
			IoEvent.IoEventType type = ioEvent.getType();
			String clientID = ioEvent.getClientID();
			switch (type) {
				case CONNECT:
					//处理不允许多终端登录的场景的策略。1:kick,2:prevent
					//如果该域下同一个账号多终端登录的策略是kick得话
					if (!ClientIds.getAccountArea(clientID).equals(Constants.SYS_AREA)) {
						long start = System.currentTimeMillis();
						int mutiClientAllowable = OnlineStateRepository.getMutiClientAllowable(clientID);
						Set<String> oldClientIDs = OnlineStateRepository.get(clientID);
						long end = System.currentTimeMillis();
						LOG.debug("mutilClient takes [{}] ms, and oldClientIDs's size is [{}]", (end - start), oldClientIDs.size());
						if (oldClientIDs.size() > 0) {
							if (Constants.KICK == mutiClientAllowable) {
								ConnectIoEvent connectIoEvent = (ConnectIoEvent) evt;
								IMClient client = connectIoEvent.getClient();
								for (String oldClientID : oldClientIDs) {
									publishForConnectConflict(oldClientID, client);
								}

							} else if (Constants.PREVENT == mutiClientAllowable) {
								//如果该域下同一个账号多终端登录的策略是prevent
								//ignore
							}
						}

					}

					// put client online
					if (!ClientIds.getAccountArea(clientID).equals(Constants.SYS_AREA)) {
						OnlineStateRepository.put(clientID);
					}

					break;

				case DISCONNECT:
					ExtraIoEvent extraIoEvent = (ExtraIoEvent) ioEvent;
					Set<Subscription> subscriptions = extraIoEvent.getSubscriptions();
//					long s1 = System.currentTimeMillis();

					for (Subscription s : subscriptions) {
						TopicRouterRepository.clean(s.getTopicFilter());
					}
//					long e1 = System.currentTimeMillis();
//					LOG.debug("DISCONNECT clean TopicNode takse [{}] ms", (e1 - s1));
					//clear onlineState
					OnlineStateRepository.remove(clientID);
					break;

				case LOSTCONNECTION:
					ExtraIoEvent connLostIoEvent = (ExtraIoEvent) ioEvent;
					Set<Subscription> lostConnSubs = connLostIoEvent.getSubscriptions();
//					long ss1 = System.currentTimeMillis();
					if (lostConnSubs != null) {
						for (Subscription s : lostConnSubs) {
							TopicRouterRepository.clean(s.getTopicFilter());
						}
					}
//					long ee1 = System.currentTimeMillis();
//					LOG.debug("Lost Connection clean TopicNode takes [{}] ms", (ee1 - ss1));
					//clear onlineState
					OnlineStateRepository.remove(clientID);

					break;
//				case PUBLISH:
//					TopicRouterRepository.remove(topic);
//					break;
			}
		} finally {
			event.setEvent(null);
		}

	}
}
