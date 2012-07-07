/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.Server;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class ConnectionPool {

		private Map<Integer, TTransport> pool = new HashMap<Integer, TTransport>();
		private Server server;
		private static final int NUM_OF_MAX_CONNECTIONS = 5;

		public ConnectionPool(Server s) {
				this.server = s;
		}

		public TTransport getConnection(int serverId, String address, int portId) {
				if (pool.containsKey(serverId)) {
						return pool.get(serverId);
				}
				if (pool.size() > NUM_OF_MAX_CONNECTIONS) {
						System.out.println(this.server.serverId + ": Looking for connections to close.");
						Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
						Set<Integer> keySet = pool.keySet();
						boolean terminate;
						int idToTerminate = -1;
						for (Integer sid : keySet) {
								terminate = true;
								for (Node node : neighbourSet) {
										if (node.serverId == sid) {
												terminate = false;
												break;
										}
								}
								if (terminate) {
										idToTerminate = sid;
										break;
								}
						}
						closeConnection(idToTerminate);
				}
				TTransport transport = new TSocket(address, portId);
				try {
						System.out.println(this.server.serverId + ": Opening connection to " + serverId + "........\t# of connections opened: " + pool.size());
						transport.open();
				} catch (TTransportException ex) {
						java.util.logging.Logger.getLogger(ConnectionPool.class.getName()).log(Level.SEVERE, null, ex);
				}
				pool.put(serverId, transport);
				return transport;
		}

		public void closeConnection(int serverId) {
				if (pool.containsKey(serverId)) {
						TTransport tTransport = pool.get(serverId);
						if (tTransport.isOpen()) {
								System.out.println(this.server.serverId + ": Clossing connection to " + serverId + "......");
								tTransport.close();
						}
						pool.remove(serverId);
				}
		}

		public void closeAllConnections() {
				Collection<TTransport> values = pool.values();
				for (TTransport tTransport : values) {
						if (tTransport.isOpen()) {
								tTransport.close();
						}
				}
				pool.clear();
		}
}
