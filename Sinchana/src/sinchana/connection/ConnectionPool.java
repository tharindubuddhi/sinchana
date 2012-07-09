/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.Server;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;

/**
 *
 * @author Hiru
 */
public class ConnectionPool {

		private Map<Integer, TTransport> pool = new HashMap<Integer, TTransport>();
		private Server server;
		private static final int NUM_OF_MAX_CONNECTIONS = 12;

		public ConnectionPool(Server s) {
				this.server = s;
		}

		public TTransport getConnection(int serverId, String address, int portId) {
				if (pool.containsKey(serverId)) {
						return pool.get(serverId);
				}

				Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
				Set<Integer> keySet = pool.keySet();
				boolean terminate;
				Set<Integer> idsToTerminate = new HashSet<Integer>();
				for (Integer sid : keySet) {
						terminate = true;
						for (Node node : neighbourSet) {
								if (node.serverId == sid) {
										terminate = false;
										break;
								}
						}
						if (terminate) {
								idsToTerminate.add(sid);
						}
				}
				for (Integer id : idsToTerminate) {
						closeConnection(id);
				}

				TTransport transport = new TSocket(address, portId);
				try {
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_CONNECTION_POOL, 0,
								"Opening connection to " + serverId + "........\t# of connections opened: " + pool.size());
						transport.open();
				} catch (TTransportException ex) {
						java.util.logging.Logger.getLogger(ConnectionPool.class.getName()).log(Level.SEVERE, null, ex);
						return null;
				}
				pool.put(serverId, transport);
				if (NUM_OF_MAX_CONNECTIONS < pool.size()) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_CONNECTION_POOL, 1,
								"Maximum number of connections opened exceeded! ("
								+ pool.size() + "/" + NUM_OF_MAX_CONNECTIONS + " are opened)");
				}
				return transport;
		}

		public void closeConnection(int serverId) {
				if (pool.containsKey(serverId)) {
						TTransport tTransport = pool.get(serverId);
						if (tTransport.isOpen()) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_CONNECTION_POOL, 2,
										"Clossing connection to " + serverId + "......");
								tTransport.close();
						}
						pool.remove(serverId);
				}
		}

		public void closeAllConnections() {
				Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_CONNECTION_POOL, 3,
						"Clossing all the connections.");
				Collection<TTransport> values = pool.values();
				for (TTransport tTransport : values) {
						if (tTransport.isOpen()) {
								tTransport.close();
						}
				}
				pool.clear();
		}
}
