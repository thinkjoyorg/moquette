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
package org.eclipse.moquette.server.netty;

import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.eclipse.moquette.proto.Utils;
import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.proto.messages.PingRespMessage;
import org.eclipse.moquette.spi.IMessaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.moquette.proto.messages.AbstractMessage.*;

/**
 *
 * @author andrea
 */
@Sharable
public class NettyMQTTHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger LOG = LoggerFactory.getLogger(NettyMQTTHandler.class);
	private final ChannelFutureListener remover = new ChannelFutureListener() {
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			remove(future.channel());
		}
	};
	private final ConcurrentHashMapV8<String, NettyChannel> m_channelMapper = new ConcurrentHashMapV8<String, NettyChannel>();
	private IMessaging m_messaging;

	@Override
	public void channelRead(final ChannelHandlerContext ctx, Object message) {
		AbstractMessage msg = (AbstractMessage) message;
		LOG.debug("Received a message of type {}", Utils.msgType2String(msg.getMessageType()));
		try {
            switch (msg.getMessageType()) {
                case CONNECT:
	            case SUBSCRIBE:
	            case UNSUBSCRIBE:
                case PUBLISH:
                case PUBREC:
                case PUBCOMP:
                case PUBREL:
                case DISCONNECT:
	            case PUBACK:
		            NettyChannel channel = m_channelMapper.computeIfAbsent(getKey(ctx.channel()), new ConcurrentHashMapV8.Fun<String, NettyChannel>() {
			            @Override
			            public NettyChannel apply(String s) {
				            ctx.channel().closeFuture().addListener(remover);
				            return new NettyChannel(ctx);
			            }
		            });
//                    synchronized(m_channelMapper) {
//                        if (!m_channelMapper.containsKey(getKey(ctx))) {
//	                        m_channelMapper.put(getKey(ctx), new NettyChannel(ctx));
//                        }
//                        channel = m_channelMapper.get(getKey(ctx));
//                    }
                    m_messaging.handleProtocolMessage(channel, msg);
                    break;
                case PINGREQ:
                    PingRespMessage pingResp = new PingRespMessage();
                    ctx.writeAndFlush(pingResp);
                    break;
            }
        } catch (Exception ex) {
            LOG.error("Bad error in processing the message", ex);
        }
    }

	private final String getKey(Channel ch) {
		return ch.toString().split(",")[0];
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.warn("NettyMQTTHandler exceptionCaught!!");
		NettyChannel channel = m_channelMapper.get(getKey(ctx.channel()));
		if (null != channel) {
			remove(ctx.channel());
			release(channel);
		}
		if (ctx.channel().isActive()) {
			ctx.close();
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
		NettyChannel channel = m_channelMapper.get(getKey(ctx.channel()));
		if (null == channel) {
			ctx.close();
			return;
		}

		remove(ctx.channel());
		release(channel);

	    ctx.close(/*false*/);

    }

	private final void remove(Channel channel) {
		if (m_channelMapper.remove(getKey(channel)) != null) {
			channel.closeFuture().removeListener(remover);
		}
	}

	private final void release(NettyChannel channel) {
		try {
			Object clientID = channel.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
			if (null != clientID) {
				LOG.debug("ClientID:[{}] is Inactive!!!", clientID.toString());
				m_messaging.lostConnection(channel, (String) clientID);
			}
		} catch (Throwable th) {
			LOG.warn("no resource to clean!", th);
		}
	}

	public void setMessaging(IMessaging messaging) {
		m_messaging = messaging;
    }

}
