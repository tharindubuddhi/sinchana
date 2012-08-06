/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import sinchana.CONFIGURATIONS;
import sinchana.Server;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;

/**
 * This class implements 
 * @author Hiru
 */
public class ConnectionPool {

		private Map<String, Connection> pool = new HashMap<String, Connection>();
		private Server server;

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
		public DHTServer.Client getConnection(String serverId, String url) {
				synchronized (this) {
						if (pool.containsKey(serverId)) {
								return pool.get(serverId).open();
						}
						Connection connection = new Connection(url);
						pool.put(serverId, connection);
						if (CONFIGURATIONS.NUM_OF_MAX_OPENED_CONNECTION < pool.size()) {
								int level = Logger.LEVEL_INFO;
								if (CONFIGURATIONS.NUM_OF_MAX_OPENED_CONNECTION + 1 < pool.size()) {
										level = Logger.LEVEL_WARNING;
								}
								Logger.log(this.server.serverId, level, Logger.CLASS_CONNECTION_POOL, 1,
										"Maximum number of connections opened exceeded! ("
										+ pool.size() + "/" + CONFIGURATIONS.NUM_OF_MAX_OPENED_CONNECTION + " are opened)");
								getSpace();
						}
						return connection.open();
				}
		}

		private void getSpace() {
				Map<String, Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
				Set<String> keySetN = neighbourSet.keySet();
				Set<String> keySet = pool.keySet();
				boolean terminate;
				String idToTerminate = null;
				long oldestTime = Long.MAX_VALUE;
				for (String sid : keySet) {
						terminate = true;
						for (String sidn : keySetN) {
								if (sidn.equals(sid)) {
										terminate = false;
										break;
								}
						}
						if (terminate && pool.get(sid).getLastUsedTime() < oldestTime) {
								oldestTime = pool.get(sid).getLastUsedTime();
								idToTerminate = sid;
						}
				}
				if (idToTerminate != null) {
						pool.get(idToTerminate).reset();
						pool.remove(idToTerminate);
				}
		}

		public void resetConnection(String id) {
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
