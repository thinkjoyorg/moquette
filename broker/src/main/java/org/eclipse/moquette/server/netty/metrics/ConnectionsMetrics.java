package org.eclipse.moquette.server.netty.metrics;

/**
 * 同时在线连接数指标
 * 创建人：xy
 * 创建时间：15/3/23
 *
 * @version 1.0
 */

public class ConnectionsMetrics {
	private long connectionsCount = 0;

	void incr(long num) {
		connectionsCount += num;
	}

	void decr(long num) {
		if (connectionsCount - num >= 0) {
			connectionsCount -= num;
		} else {
			connectionsCount = 0;
		}
	}

	public long getConnectionsCount() {
		return connectionsCount;
	}
}
