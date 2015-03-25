package org.eclipse.moquette.server.netty.metrics;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * 创建人：xy
 * 创建时间：15/3/23
 *
 * @version 1.0
 */

public class ConnectionsMetricsHandler extends ChannelDuplexHandler {

	private static final AttributeKey<ConnectionsMetrics> ATTR_KEY_METRICS = AttributeKey.valueOf("ConnectionsMetrics");

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		Attribute<ConnectionsMetrics> attr = ctx.attr(ATTR_KEY_METRICS);
		ConnectionsMetrics connectionsMetrics = new ConnectionsMetrics();
		connectionsMetrics.incr(1);
		attr.set(connectionsMetrics);
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		Attribute<ConnectionsMetrics> attr = ctx.attr(ATTR_KEY_METRICS);
		ConnectionsMetrics connectionsMetrics = new ConnectionsMetrics();
		connectionsMetrics.decr(1);
		attr.set(connectionsMetrics);
		super.channelInactive(ctx);
	}
}
