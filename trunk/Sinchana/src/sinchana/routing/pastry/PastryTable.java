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
package sinchana.routing.pastry;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import sinchana.RoutingHandler;
import sinchana.SinchanaServer;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import sinchana.SinchanaDHT;
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class PastryTable implements RoutingHandler {

	private static final int SUCCESSOR_LEVELS = 8;
	private static final int NUMBER_OF_TABLE_ENTRIES = 3;
	private static final int BASE_POWER = 4;
	private static final int TABLE_WIDTH = (int) Math.pow(2, BASE_POWER);
	private static final int TABLE_SIZE = 40;
	private static final BigInteger ZERO = new BigInteger("0", 16);
	private final SinchanaServer server;
	private final Node[] successors = new Node[SUCCESSOR_LEVELS];
	private final Node[] predecessors = new Node[SUCCESSOR_LEVELS];
	private final byte[] serverId;
	private final BigInteger serverIdAsBigInt;
	private final Node thisNode;
	private final Node[][][] routingTable = new Node[TABLE_SIZE][TABLE_WIDTH][NUMBER_OF_TABLE_ENTRIES];
	private final Timer timer = new Timer();
	private int timeOutCount = 0;

	public PastryTable(SinchanaServer svr) {
		this.server = svr;
		this.thisNode = server.getNode();
		this.serverId = thisNode.serverId.array();
		this.serverIdAsBigInt = new BigInteger(1, serverId);
		this.timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (++timeOutCount >= SinchanaDHT.ROUTING_OPTIMIZATION_TIME_OUT) {
					timeOutCount = 0;
					optimize();
				}
			}
		}, 1000, 1000);
	}

	@Override
	public void init() {
		this.initRoutingTable();
	}
    
    /**
	 * Build routing table and successor and predecessor list.
	 * Each routing table entry contains one primary link and two backup links.
	 */
	private void initRoutingTable() {
		synchronized (routingTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				for (int j = 0; j < TABLE_WIDTH; j++) {
					for (int k = 0; k < NUMBER_OF_TABLE_ENTRIES; k++) {
						routingTable[i][j][k] = null;
					}
				}
			}
		}
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				predecessors[i] = null;
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				successors[i] = null;
			}
		}
	}

	@Override
	public void triggerOptimize() {
		timeOutCount = SinchanaDHT.ROUTING_OPTIMIZATION_TIME_OUT;
	}

	private void optimize() {
		Message msg = new Message(MessageType.DISCOVER_NEIGHBORS, thisNode, 2);
		msg.setFailedNodeSet(server.getConnectionPool().getFailedNodes());
		msg.setNeighbourSet(getNeighbourSet());
		for (Node node : predecessors) {
			if (node == null) {
				break;
			}
			synchronized (node) {
				this.server.getIOHandler().send(msg.deepCopy(), node);
				break;
			}
		}
		for (Node node : successors) {
			if (node == null) {
				break;
			}
			synchronized (node) {
				this.server.getIOHandler().send(msg.deepCopy(), node);
				break;
			}
		}
		timeOutCount = 0;
	}

	@Override
	public Node[] getSuccessors() {
		return successors;
	}

	@Override
	public Node[] getPredecessors() {
		return predecessors;
	}

	@Override
	public Node getNextNode(byte[] destination) {
		Node nodeFromLeafSet = checkInLeafSet(destination);
		if (nodeFromLeafSet != null) {
			return nodeFromLeafSet;
		}
		int row = getRow(this.serverId, destination);
		if (row == -1) {
			return thisNode;
		}
		int column = getColumn(destination, row);
        
        /* if routing table has an entry matching dgreatest common prefix with the destination returns it.*/
		synchronized (routingTable) {
			for (Node node : routingTable[row][column]) {
				if (node != null) {
					return node;
				}
			}

			int tColumn, iRow, iColumn;

			iRow = row;
			iColumn = column;
			tColumn = getColumn(this.serverId, row);
			boolean traverseDown = column < tColumn;
			if (row == 0) {
				traverseDown = false;
			}
            
            /* travels down if server node id is stored in a larger column number */
			if (row == TABLE_SIZE - 1) {
				traverseDown = true;
			}

			while (true) {
				for (Node node : routingTable[row][column]) {
					if (node != null) {
						return node;
					}
				}

				tColumn = getColumn(this.serverId, row);

				if (traverseDown && tColumn == column) {
					row--;
					traverseDown = row != 0;
					column = -1;
				} else if (!traverseDown && column == TABLE_WIDTH - 1) {
					row++;
					traverseDown = row >= TABLE_SIZE - 1;
					column = getColumn(this.serverId, row);
				}
                
                /* case where node responsible for the destination is this node.*/
				column = (column + 1) % TABLE_WIDTH;
				if (iRow == row && iColumn == column) {
					return thisNode;
				}
			}
		}
	}

	@Override
	public boolean isInTheTable(Node nodeToCkeck) {
		byte[] id = nodeToCkeck.serverId.array();
		int row = getRow(this.serverId, id);
		int column = getColumn(id, row);
        
        /* check whether the node is within predessesors. */
		for (int i = 0; i < NUMBER_OF_TABLE_ENTRIES; i++) {
			if (routingTable[row][column][i] == null) {
				break;
			}
			if (Arrays.equals(routingTable[row][column][i].serverId.array(), id)) {
				return true;
			}
		}
        
        /* check whether the node is within predessesors. */
		for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
			if (predecessors[i] == null) {
				break;
			}
			if (Arrays.equals(predecessors[i].serverId.array(), id)) {
				return true;
			}
		}
        
        /* check whether the node is within successors. */
		for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
			if (successors[i] == null) {
				break;
			}
			if (Arrays.equals(successors[i].serverId.array(), id)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Set<Node> getNeighbourSet() {
        
		/*initializes an empty thisNode set.*/
		Set<Node> neighbourSet = new HashSet<Node>();
		synchronized (routingTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				for (int j = 0; j < TABLE_WIDTH; j++) {
					for (int k = 0; k < NUMBER_OF_TABLE_ENTRIES; k++) {
						if (routingTable[i][j][k] != null) {
							neighbourSet.add(routingTable[i][j][k]);
						}
					}
				}
			}
		}
        
		/*adds the predecessors & the successors. */
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (predecessors[i] != null) {
					neighbourSet.add(this.predecessors[i]);
				}
			}
		}
        
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (successors[i] != null) {
					neighbourSet.add(this.successors[i]);
				}
			}
		}
		
		return neighbourSet;
	}

	@Override
	public boolean updateTable(Node node, boolean add) {
		if (Arrays.equals(node.serverId.array(), this.serverId)) {
			return false;
		}
		boolean updated;
		if (add) {
			updated = addNode(node);
		} else {
			updated = removeNode(node);
		}
		if (updated) {
			timeOutCount = 0;
		}
		return updated;
	}
    
     /**
	 * Add new node to the routing table and to predecessors or successors.
     * Returns whether the node is added into the routing table or to the to predecessors or successors or not.
     * @param node	Node that to added to from routing table and successors and predecessors.
	 * @return <code>true</code> the node is added into the routing table or to the to 
     * predecessors or successors. <code>false</code> otherwise.
	 * 
	 */
	private boolean addNode(Node node) {
		boolean updatedSuccessors = false, updatedPredecessors = false, updatedTable = false;
		BigInteger successorOffset, pointerOffset, tempNodeOffset, newNodeOffset = getOffset(node.serverId.array());
		Node tempNodeToUpdate;
		synchronized (successors) {
			pointerOffset = ZERO;
			tempNodeToUpdate = node;
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
                
				/**
				 * Checks whether the new thisNode should be set as the successors or not. 
				 * if successors is the server itself (successorOffset == 0) or if the new thisNode 
				 * is not the server it self (newNodeOffset != 0) and it successes 
				 * the server than the existing successors (newNodeOffset < successorOffset), 
				 * the new thisNode will be set as the successors.
				 * 0-----this.server.id------new.server.id-------existing.successors.id----------End.of.Grid
				 */
				successorOffset = successors[i] == null ? ZERO : getOffset(successors[i].serverId.array());
				if (updatedSuccessors 
						|| (pointerOffset.compareTo(newNodeOffset) == -1
						&& (successorOffset.equals(ZERO) || newNodeOffset.compareTo(successorOffset) == -1))) {
					Node temp = this.successors[i];
					this.successors[i] = tempNodeToUpdate.deepCopy();
					tempNodeToUpdate = temp;
					updatedSuccessors = true;
				} else {
					pointerOffset = successorOffset;
				}
				if (tempNodeToUpdate == null || successors[i] == null) {
					break;
				}
			}
		}
		synchronized (predecessors) {
			pointerOffset = SinchanaServer.GRID_SIZE;
			tempNodeToUpdate = node;
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
                
				/**
				 * Checks whether the new thisNode should be set as the predecessors or not. 
				 * if predecessors is the server itself (predecessorOffset == 0) or if the new thisNode 
				 * is not the server it self (newNodeOffset != 0) and it predecessors 
				 * the server than the existing predecessors (predecessorOffset < newNodeOffset), 
				 * the new thisNode will be set as the predecessors.
				 * 0-----existing.predecessors.id------new.server.id-------this.server.id----------End.of.Grid
				 */
				successorOffset = predecessors[i] == null ? ZERO : getOffset(predecessors[i].serverId.array());
				if (updatedPredecessors 
						|| (newNodeOffset.compareTo(pointerOffset) == -1
						&& (successorOffset.equals(ZERO) || successorOffset.compareTo(newNodeOffset) == -1))) {
					Node temp = this.predecessors[i];
					this.predecessors[i] = tempNodeToUpdate.deepCopy();
					tempNodeToUpdate = temp;
					updatedPredecessors = true;
				} else {
					pointerOffset = successorOffset;
				}
				if (tempNodeToUpdate == null || predecessors[i] == null) {
					break;
				}
			}
		}
		int row = getRow(this.serverId, node.serverId.array());
		int column = getColumn(node.serverId.array(), row);
		synchronized (routingTable) {
			for (int i = 0; i < NUMBER_OF_TABLE_ENTRIES; i++) {
				if (routingTable[row][column][i] != null && Arrays.equals(routingTable[row][column][i].serverId.array(), node.serverId.array())) {
					break;
				} else if (routingTable[row][column][i] == null) {
					routingTable[row][column][i] = node.deepCopy();
					updatedTable = true;
					break;
				}
			}
		}
		return updatedPredecessors || updatedSuccessors || updatedTable;
	}
    
     /**
	 * Remove a node from the routing table and neighbor set
     * Returns whether the node is removed from the routing table and neighbor set.
     * @param nodeToRemove	Node that to be removed from routing table and successors and predecessors.
	 * @return <code>true</code> node is removed from the routing table and neighbor set. <code>false</code> otherwise.
	 */
	private boolean removeNode(Node nodeToRemove) {
		Set<Node> neighbourSet = getNeighbourSet();
		if (neighbourSet.contains(nodeToRemove)) {
			neighbourSet.remove(nodeToRemove);
            
			/*reset the predecessors, successors and finger table entries.*/
			initRoutingTable();
			for (Node node : neighbourSet) {
				addNode(node);
			}
//			Logger.log(thisNode, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 1,
//					"Node " + ByteArrays.idToReadableString(nodeToRemove)
//					+ " @ " + nodeToRemove.address + " is removed from the routing table!");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Returns the offset of the id relative to the this server.
	 * <code>(id + RoutingHandler.GRID_SIZE - this.server.serverId) % RoutingHandler.GRID_SIZE;</code>
	 * @param id	Id to calculate the offset.
	 * @return		Offset of the id relative to this server.
	 */
	private BigInteger getOffset(byte[] id) {
		for (int i = 0; i < 20; i++) {
			if ((this.serverId[i] + 256) % 256 > (id[i] + 256) % 256) {
				return SinchanaServer.GRID_SIZE.add(new BigInteger(1, id)).subtract(serverIdAsBigInt);
			} else if ((this.serverId[i] + 256) % 256 < (id[i] + 256) % 256) {
				return new BigInteger(1, id).subtract(serverIdAsBigInt);
			}
		}
		return ZERO;
	}
    
    /**
	 * Match two Id to find the largest common prefix and calculate row number 
     * where the new node must be inserted in the routing table.
	 * 
	 * @param id	Id of this node.
     * @param id	Id of the new node.
	 * @return		row number of the routing table.
	 */
	private int getRow(byte[] id, byte[] newId) {
		int x1, x2;
		for (int i = 0; i < 20; i++) {
			x1 = id[i];
			x2 = newId[i];
            
             /* check every bit for in every byte in the byte array to get the greatest common prefix match. */
			for (int j = 7; j >= 0; j--) {
				if ((x1 & (1 << j)) != (x2 & (1 << j))) {
					return ((8 / BASE_POWER) * (20 - i) - ((8 / BASE_POWER) - (int) (j / BASE_POWER)));
				}
			}
		}
		return -1;
	}
    
    /**
	 *Returns the column number for a given row number and Server Id.
	 * 
	 * @param id	Id of this node.
     * @param row	row number of the routing table.
	 * @return		column number of the routing table.
	 */
	private int getColumn(byte[] id, int row) {
		int val = (id[20 - (row * BASE_POWER / 8) - 1] + 256) % 256;
        
        /* find the next digit of the destination address that need to be match. */
		val = (int) (val / Math.pow(TABLE_WIDTH, row % (8 / BASE_POWER)));
		return val % TABLE_WIDTH;
	}

	@Override
	public void printInfo() {
		final String spaces = "****************************************";
		System.out.println("--------------" + this.server.getServerIdAsString() + "--------------");
		System.out.println("Routing Table");
		synchronized (routingTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				for (int j = 0; j < TABLE_WIDTH; j++) {
					System.out.print(" |");
					for (int k = 0; k < NUMBER_OF_TABLE_ENTRIES; k++) {
						if (routingTable[i][j][k] != null) {
							System.out.print(" " + ByteArrays.idToReadableString(routingTable[i][j][k].serverId));
						} else {
							System.out.print(" " + spaces);
						}
					}
					System.out.print(" |");
				}
				System.out.println("");
			}
		}
		System.out.println("Predecessors");
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (predecessors[i] != null) {
					System.out.print(" " + ByteArrays.idToReadableString(predecessors[i].serverId));
				} else {
					System.out.print(" " + spaces);
				}
			}
		}
		System.out.println("\nSuccessors");
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (successors[i] != null) {
					System.out.print(" " + ByteArrays.idToReadableString(successors[i].serverId));
				} else {
					System.out.print(" " + spaces);
				}
			}
		}
		System.out.println("\n");
	}

	private Node checkInLeafSet(byte[] destinationId) {
		BigInteger nodeOffset, destinationOffset = getOffset(destinationId);
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (successors[i] == null) {
					break;
				}
				nodeOffset = getOffset(successors[i].serverId.array());
				if (destinationOffset.compareTo(nodeOffset) != 1) {
					return successors[i];
				}
			}
		}
		Node temp = thisNode;
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (predecessors[i] == null) {
					break;
				}
				nodeOffset = getOffset(predecessors[i].serverId.array());
				if (nodeOffset.compareTo(destinationOffset) == -1) {
					return temp;
				}
				temp = predecessors[i];
			}
		}
		return null;
	}
}
