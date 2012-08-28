/************************************************************************************

 * Sinchana Distributed Hash table 

 * Copyright (C) 2012 Sinchana DHT - Department of Computer Science &               
 * Engineering, University of Moratuwa, Sri Lanka. Permission is hereby 
 * granted, free of charge, to any person obtaining a copy of this 
 * software and associated documentation files of Sinchana DHT, to deal 
 * in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.

 * Neither the name of University of Moratuwa, Department of Computer Science 
 * & Engineering nor the names of its contributors may be used to endorse or 
 * promote products derived from this software without specific prior written 
 * permission.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
 * SOFTWARE.                                                                    
 ************************************************************************************/
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
		if (pool.containsKey(node.address)) {
			return pool.get(node.address);
		}
		Connection connection = new Connection(node);
		if (SinchanaDHT.NODE_POOL_SIZE <= pool.size()) {
			getSpaceForNodes();
		}
		int numberOfOpenedConnections = getNumberOfOpenedConnections();
		while (SinchanaDHT.NUM_OF_MAX_OPENED_CONNECTION <= numberOfOpenedConnections) {
			getSpaceForConnections();
			numberOfOpenedConnections = getNumberOfOpenedConnections();
		}
		pool.put(node.address, connection);
		connection.getClient();
		return connection;
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
		synchronized (pool) {
			return pool.containsKey(node.address) && pool.get(node.address).isFailed();
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
