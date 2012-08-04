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
import sinchana.Server;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;

/**
 * This class implements 
 * @author Hiru
 */
public class ConnectionPool {

		private Map<Long, Connection> pool = new HashMap<Long, Connection>();
		private Server server;
		private static final int NUM_OF_MAX_CONNECTIONS = 18;

		/**
		 * Class constructor. The server instance where the connection fool is 
		 * initialized is passed as the argument
		 * @param s		Server instance.
		 */
		public ConnectionPool(Server s) {
				this.server = s;
		}

		/**
		 * Returns the connection to the given destination. If the connection is
		 * already opened and in the connection pool, it returns. Otherwise, open 
		 * the connection to the destination and adds it to the connection pool.
		 * @param serverId		Server id of the destination.
		 * @param address		URL of the destination.
		 * @param portId		Port id of the destination.
		 * @return				TTransport connection opened to the destination.
		 */
		public DHTServer.Client getConnection(long serverId, String address, int portId) {
				synchronized (this) {
						if (pool.containsKey(serverId)) {
								return pool.get(serverId).open();
						}
						Connection connection = new Connection(address, portId);
						pool.put(serverId, connection);
						if (NUM_OF_MAX_CONNECTIONS < pool.size()) {
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_CONNECTION_POOL, 1,
										"Maximum number of connections opened exceeded! ("
										+ pool.size() + "/" + NUM_OF_MAX_CONNECTIONS + " are opened)");
								getSpace();
						}
						return connection.open();
				}
		}

		private void getSpace() {
				Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
				Set<Long> keySet = pool.keySet();
				boolean terminate;
				Set<Long> idsToTerminate = new HashSet<Long>();
				for (long sid : keySet) {
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
				for (Long id : idsToTerminate) {
						pool.get(id).reset();
						pool.remove(id);
				}
		}

		public void resetConnection(long id) {
				synchronized (this) {
						pool.get(id).reset();
						pool.remove(id);
				}
		}

		/**
		 * Closes all the connections in the connection pool.
		 */
		public void closeAllConnections() {
				synchronized (this) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_CONNECTION_POOL, 3,
								"Clossing all the connections.");
						Collection<Connection> values = pool.values();
						for (Connection connection : values) {
								connection.reset();
						}
						pool.clear();
				}
		}
}
