/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import sinchana.thrift.Node;

/**
 * This class implements 
 * @author Hiru
 */
public class ConnectionPool {

	private final ConcurrentHashMap<String, Connection> pool = new ConcurrentHashMap<String, Connection>();
	private SinchanaServer server;

	/**
	 * Class constructor. The server instance where the connection fool is 
	 * initialized is passed as the argument
	 * @param s		SinchanaServer instance.
	 */
	ConnectionPool(SinchanaServer svr) {
		this.server = svr;
	}

	synchronized Connection getConnection(Node node) {
		String id = new String(node.serverId.array());
		if (pool.containsKey(id)) {
			return pool.get(id);
		}
		Connection connection = new Connection(node);
		if (CONFIGURATIONS.NODE_POOL_SIZE <= pool.size()) {
			getSpaceForNodes();
		}
		int numberOfOpenedConnections = getNumberOfOpenedConnections();
		while (CONFIGURATIONS.NUM_OF_MAX_OPENED_CONNECTION <= numberOfOpenedConnections) {
			getSpaceForConnections();
			numberOfOpenedConnections = getNumberOfOpenedConnections();
		}
		pool.put(id, connection);
		connection.getClient();
		return connection;
	}

	boolean isAlive(byte[] nodeId) {
		Connection connection = pool.get(new String(nodeId));
		return connection.isAlive();
	}

	private int getNumberOfOpenedConnections() {
		int count = 0;
		Collection<Connection> values = pool.values();
		for (Connection connection : values) {
			if (connection.isOpened()) {
				count++;
			}
		}
		return count;
	}

	boolean hasReportFailed(Node node) {
		String id = new String(node.serverId.array());
		synchronized (pool) {
			return pool.containsKey(id) && pool.get(id).isFailed();
		}
	}

	private void getSpaceForNodes() {
		Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
		Set<String> keySet = pool.keySet();
		boolean terminate;
		String idToTerminate = null;
		long oldestTime = Long.MAX_VALUE;
		for (String sid : keySet) {
			terminate = true;
			for (Node node : neighbourSet) {
				if (Arrays.equals(node.serverId.array(), sid.getBytes())) {
					terminate = false;
					break;
				}
			}
			long lut = pool.get(sid).getLastUsedTime();
			if (terminate && lut < oldestTime) {
				oldestTime = lut;
				idToTerminate = sid;
			}
		}
		if (idToTerminate != null) {
			pool.get(idToTerminate).close();
			pool.remove(idToTerminate);
		}
	}

	private void getSpaceForConnections() {
		Connection connectionToTerminate = null;
		long oldestTime = Long.MAX_VALUE;
		Collection<Connection> values = pool.values();
		for (Connection connection : values) {
			if (connection.isOpened() && connection.getLastUsedTime() < oldestTime) {
				oldestTime = connection.getLastUsedTime();
				connectionToTerminate = connection;
			}
		}
		connectionToTerminate.close();
	}

	/**
	 * Closes all the connections in the connection pool.
	 */
	void closeAllConnections() {
		synchronized (pool) {
			Collection<Connection> values = pool.values();
			for (Connection connection : values) {
				connection.close();
			}
			pool.clear();
		}
	}
	
	boolean updateNodeInfo(Node node, boolean alive){
		Connection connection = getConnection(node);
		return connection.updateInfo(alive);
	}

	public Set<Node> getFailedNodes() {
		Set<Node> nodes = new HashSet<Node>();
		Collection<Connection> values = pool.values();
		for (Connection connection : values) {
			if (connection.isFailed()) {
				nodes.add(connection.getNode());
			}
		}
		return nodes;
	}
}
