/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Calendar;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class FaultHandler {

	private static final int MAX_POOL_SIZE = 12;
	private final ConcurrentHashMap<String, NodeInfoContainer> nodePool = new ConcurrentHashMap<String, NodeInfoContainer>();
	private final ConcurrentHashMap<String, Connection> connectionPool = new ConcurrentHashMap<String, Connection>();
	private final Server server;
	
	public FaultHandler(Server svr){
		this.server = svr;
	}

	public class NodeInfoContainer {

		Node node;
		long lastKnownFailedTime;
		long lastHeardFailedTime;
		long lastUsedTime;
		long lastOpenedTime;
		long rounTripTime;
		int numOfOpenTries;
		boolean joinedBackSinceLastFail;
		boolean opened;
	}

	public class Connection {

		private DHTServer.Client client;
		private TTransport transport;
		private TProtocol protocol;
	}

	public DHTServer.Client open(Node node) {
		NodeInfoContainer nic = getNodeInfoContainer(node);
		Connection connection = getConnection(node);
		nic.lastUsedTime = Calendar.getInstance().getTimeInMillis();
		if (nic.opened) {
			return connection.client;
		}
		nic.lastOpenedTime = Calendar.getInstance().getTimeInMillis();
		try {
			connection.transport = new TSocket(nic.node.address.split(":")[0], Integer.parseInt(nic.node.address.split(":")[1]));
			connection.transport.open();
			connection.protocol = new TBinaryProtocol(connection.transport);
			connection.client = new DHTServer.Client(connection.protocol);
			nic.numOfOpenTries = 0;
			nic.opened = true;
			return connection.client;
		} catch (TTransportException ex) {
			nic.numOfOpenTries++;
			nic.opened = false;
			nic.lastKnownFailedTime = Calendar.getInstance().getTimeInMillis();
			nic.joinedBackSinceLastFail = false;
			return null;
		}
	}

	public NodeInfoContainer getNodeInfoContainer(Node node) {
		if (nodePool.containsKey(node.serverId)) {
			return nodePool.get(node.serverId);
		} else {
			if (nodePool.size() >= MAX_POOL_SIZE) {
			}
			NodeInfoContainer nic = new NodeInfoContainer();
			nic.node = node.deepCopy();
			nic.joinedBackSinceLastFail = false;
			nic.opened = false;
			nic.rounTripTime = -1;
			nic.lastHeardFailedTime = -1;
			nic.lastKnownFailedTime = -1;
			nic.lastOpenedTime = -1;
			nic.lastUsedTime = -1;
			nic.numOfOpenTries = 0;
			nodePool.put(node.serverId, nic);
			return nic;
		}
	}

	public Connection getConnection(Node node) {
		if (connectionPool.containsKey(node.serverId)) {
			return connectionPool.get(node.serverId);
		} else {
			if (connectionPool.size() >= MAX_POOL_SIZE) {
			}
			Connection c = new Connection();
			connectionPool.put(node.serverId, c);
			return c;
		}
	}
}
