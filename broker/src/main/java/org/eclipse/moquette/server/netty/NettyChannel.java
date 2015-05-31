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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.eclipse.moquette.server.Constants;
import org.eclipse.moquette.server.ServerChannel;

/**
 *
 * @author andrea
 */
public class NettyChannel implements ServerChannel {

	public static final AttributeKey<Object> ATTR_KEY_KEEPALIVE = AttributeKey.valueOf(Constants.KEEP_ALIVE);
	public static final AttributeKey<Object> ATTR_KEY_CLEANSESSION = AttributeKey.valueOf(Constants.CLEAN_SESSION);
	public static final AttributeKey<Object> ATTR_KEY_CLIENTID = AttributeKey.valueOf(Constants.ATTR_CLIENTID);
	private ChannelHandlerContext m_channel;

	NettyChannel(ChannelHandlerContext ctx) {
		m_channel = ctx;
	}

	public Object getAttribute(AttributeKey<Object> key) {
		Attribute<Object> attr = m_channel.attr(key);
		return attr.get();
	}

	public void setAttribute(AttributeKey<Object> key, Object value) {
		Attribute<Object> attr = m_channel.attr(key);
		attr.set(value);
	}
    
    public void setIdleTime(int idleTime) {
        if (m_channel.pipeline().names().contains("idleStateHandler")) {
            m_channel.pipeline().remove("idleStateHandler");
        }
        if (m_channel.pipeline().names().contains("idleEventHandler")) {
            m_channel.pipeline().remove("idleEventHandler");
        }
        m_channel.pipeline().addFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTime));
        m_channel.pipeline().addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
    }

    public void close(boolean immediately) {
        m_channel.close();
    }

    public void write(Object value) {
        m_channel.writeAndFlush(value);
    }

	@Override
	public String toString() {
		String clientID = (String) getAttribute(ATTR_KEY_CLIENTID);
		return "session [clientID: " + clientID + "]" + super.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		NettyChannel that = (NettyChannel) o;

		if (m_channel != null ? !getKey(m_channel.channel()).equals(getKey(that.m_channel.channel())) : that.m_channel != null)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return m_channel != null ? getKey(m_channel.channel()).hashCode() : 0;
	}

	private final String getKey(Channel channel) {
		return channel.toString().split(",")[0];
	}
}
