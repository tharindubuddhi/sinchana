/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import sinchana.CONFIGURATIONS;
import sinchana.SinchanaServer;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;

/**
 * This class implements 
 * @author Hiru
 */
public class ConnectionPool {
	
	private final Map<String, Connection> pool = new HashMap<String, Connection>();
	private SinchanaServer server;

	/**
	 * Class constructor. The server instance where the connection fool is 
	 * initialized is passed as the argument
	 * @param s		SinchanaServer instance.
	 */
	public ConnectionPool(SinchanaServer svr) {
		this.server = svr;
	}

	/**
	 * Returns the connection to the given destination. If the connection is
	 * already opened and in the connection pool, it returns. Otherwise, open 
	 * the connection to the destination and adds it to the connection pool.
	 * @param serverId		SinchanaServer id of the destination.
	 * @param address		URL of the destination.
	 * @param portId		Port id of the destination.
	 * @return				TTransport connection opened to the destination.
	 */
	public Connection getConnection(Node node) {
		String id = new String(node.getServerId());
		synchronized (pool) {
			if (pool.containsKey(id)) {
				return pool.get(id);
			} else {
				Connection connection = new Connection(node);
				int numberOfOpenedConnections = getNumberOfOpenedConnections();
				if (CONFIGURATIONS.NODE_POOL_SIZE <= pool.size()) {
					Logger.log(this.server.getNode(), Logger.LEVEL_WARNING, Logger.CLASS_CONNECTION_POOL, 1,
							"Maximum number of nodes available exceeded! ("
							+ numberOfOpenedConnections + "/" + pool.size() + ")");
					getSpaceForNodes();
				}
				numberOfOpenedConnections = getNumberOfOpenedConnections();
				while (CONFIGURATIONS.NUM_OF_MAX_OPENED_CONNECTION <= numberOfOpenedConnections) {
//					Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_CONNECTION_POOL, 1,
//							"Maximum number of connections opened exceeded! ("
//							+ numberOfOpenedConnections + "/" + pool.size() + " are opened).");
					getSpaceForConnections();
					numberOfOpenedConnections = getNumberOfOpenedConnections();
				}
				pool.put(id, connection);
				connection.getClient();
				return connection;
			}
		}
	}
	
	public boolean isAlive(byte[] nodeId) {
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
	
	public boolean hasReportFailed(Node node) {
		boolean result = false;
		String id = new String(node.getServerId());
		synchronized (pool) {
			result = pool.containsKey(id) && pool.get(id).isFailed();
		}
		return result;
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
				if (Arrays.equals(node.getServerId(), sid.getBytes())) {
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
	public void closeAllConnections() {
		synchronized (pool) {
			Collection<Connection> values = pool.values();
			for (Connection connection : values) {
				connection.close();
			}
			pool.clear();
		}
	}
	
	public void updateFailedNodeInfo(Set<Node> failedNodeSet) {
		for (Node node : failedNodeSet) {
			Connection connection = getConnection(node);
			connection.failedByInfo();
		}
	}
	
	public void updateFailedNodeInfo(Node node) {
		Connection connection = getConnection(node);
		connection.failedByInfo();
	}
	
	public Set<Node> getFailedNodes() {
		Set<Node> nodes = new HashSet<Node>();
		synchronized (pool) {
			Collection<Connection> values = pool.values();
			for (Connection connection : values) {
				if (connection.isFailed()) {
					nodes.add(connection.getNode());
				}
			}
		}
		return nodes;
	}
}
