package org.eclipse.moquette.server.cluster;

import java.io.Serializable;

/**
 * 创建人：xy
 * 创建时间：15/3/10
 *
 * @version 1.0
 */

public class Node implements Serializable {

	/* mqtt集群的节点id，值为1 - 1024(包含1和1024本身)之间的整数 */
	private int id;
	/* mqtt集群节点的ip */
	private String host;
	/* mqtt集群几点的端口 */
	private int port;

	public Node(int id, String host, int port) {
		this.id = id;
		this.host = host;
		this.port = port;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Node)) return false;

		Node node = (Node) o;

		if (id != node.id) return false;
		if (port != node.port) return false;
		if (!host.equals(node.host)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + host.hashCode();
		result = 31 * result + port;
		return result;
	}

	public String getNodeUri() {
		return "tcp://" + host + ":" + port;
	}
}
