package org.eclipse.moquette.spi.impl;

import java.util.Set;

import cn.thinkjoy.im.api.client.IMClient;
import cn.thinkjoy.im.common.ClientIds;
import cn.thinkjoy.im.protocol.system.KickOrder;
import com.lmax.disruptor.WorkHandler;
import org.eclipse.moquette.commons.Constants;
import org.eclipse.moquette.proto.MQTTException;
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

	static void closeIMClient() {
		client.shutdown();
	}

	/**
	 * publish to connect conflict message kickOrder
	 */
	private final void publishForConnectConflict(String clientID) {
		LOG.info("publishForConnectConflict for client [{}]", clientID);
		try {
			String from = ClientIds.getAccount(clientID);
			String areaAccount = ClientIds.getAccountArea(clientID);
			KickOrder kickOrder = new KickOrder(areaAccount, from, from, clientID, null);
			client.kicker().kick(kickOrder);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new MQTTException(e);
		}
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
						int mutiClientAllowable = OnlineStateRepository.getMutiClientAllowable(clientID);
						Set<String> oldClientIDs = OnlineStateRepository.get(clientID);
						if (oldClientIDs.size() > 0) {
							if (Constants.KICK == mutiClientAllowable) {
								for (String oldClientID : oldClientIDs) {
									publishForConnectConflict(oldClientID);
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

					for (Subscription s : subscriptions) {
						TopicRouterRepository.clean(s.getTopicFilter());
					}
					//clear onlineState
					OnlineStateRepository.remove(clientID);
					break;

				case LOSTCONNECTION:
					ExtraIoEvent connLostIoEvent = (ExtraIoEvent) ioEvent;
					Set<Subscription> lostConnSubs = connLostIoEvent.getSubscriptions();
					if (lostConnSubs != null) {
						for (Subscription s : lostConnSubs) {
							TopicRouterRepository.clean(s.getTopicFilter());
						}
					}
					//clear onlineState
					OnlineStateRepository.remove(clientID);

					break;
//				case PUBLISH:
//					TopicRouterRepository.remove(topic);
//					break;
			}
		} catch (Throwable th) {
			LOG.error(th.getMessage(), th);
		} finally {
			/**
			 * 防止内存溢出
			 */
			event.setEvent(null);
		}

	}
}
