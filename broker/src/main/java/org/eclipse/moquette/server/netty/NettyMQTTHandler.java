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

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
		            m_messaging.handleProtocolMessage(new NettyChannel(ctx), msg);
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
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.warn("NettyMQTTHandler exceptionCaught!");
		try {
            release(ctx);
        } catch (Throwable th) {
			LOG.warn("no resource to clean!", th);
		} finally {
			if (ctx.channel().isActive()) ctx.close(/*false*/);
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		try {
			release(ctx);
		} catch (Throwable th) {
			LOG.warn("no resource to clean!", th);
		} finally {
			ctx.close(/*false*/);
		}

    }

	private void release(ChannelHandlerContext ctx) {
		String clientID = (String) NettyUtils.getAttribute(ctx, NettyChannel.ATTR_KEY_CLIENTID);
		if (clientID != null) {
			m_messaging.lostConnection(clientID);
		}
	}

	public void setMessaging(IMessaging messaging) {
		m_messaging = messaging;
    }

}
