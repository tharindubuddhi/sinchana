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
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.Server;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;

/**
 * This class implements 
 * @author Hiru
 */
public class ConnectionPool {

		private Map<Long, TTransport> pool = new HashMap<>();
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
		public TTransport getConnection(long serverId, String address, int portId) {
				if (pool.containsKey(serverId)) {
						return pool.get(serverId);
				}

				Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
				Set<Long> keySet = pool.keySet();
				boolean terminate;
				Set<Long> idsToTerminate = new HashSet<>();
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
						closeConnection(id);
				}

				TTransport transport = new TSocket(address, portId);
				try {
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_CONNECTION_POOL, 0,
								"Opening connection to " + serverId + "........\t# of connections opened: " + pool.size());
						transport.open();
				} catch (TTransportException ex) {
						System.out.println("msg " + ex.getLocalizedMessage());
						System.out.println("type is " + ex.getType());
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

		/**
		 * Closes the connection to the given destination, if it is available in 
		 * the connection pool.  
		 * @param serverId		Server id to close the connection.
		 */
		public void closeConnection(long serverId) {
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

		/**
		 * Closes all the connections in the connection pool.
		 */
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
