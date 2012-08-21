/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.pastry;

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
import sinchana.CONFIGURATIONS;

/**
 *
 * @author Hiru
 */
public class PastryTable implements RoutingHandler {

	public static final int PASTRY_TABLE_NUMBER_BASE = 16;
	public static final int TABLE_SIZE = 40;
	private static final int SUCCESSOR_LEVEL = 3;
	private static final int NUMBER_OF_ENTRIES = 3;
	private static final BigInteger ZERO = new BigInteger("0", CONFIGURATIONS.NUMBER_BASE);
	private static final BigInteger FACTOR = new BigInteger(Integer.toString(PASTRY_TABLE_NUMBER_BASE));
	private final SinchanaServer server;
	private final Node[] successors = new Node[SUCCESSOR_LEVEL];
	private final Node[] predecessors = new Node[SUCCESSOR_LEVEL];
	private byte[] serverId;
	private BigInteger serverIdAsBigInt;
	private final Node[][][] fingerTable = new Node[TABLE_SIZE][PASTRY_TABLE_NUMBER_BASE][NUMBER_OF_ENTRIES];
	private final Timer timer = new Timer();
	private int timeOutCount = 0;

	/**
	 * Class constructor with the server instance where the routing table is initialize.
	 * @param server		SinchanaServer instance. The routing table will be initialize based on this.
	 */
	public PastryTable(SinchanaServer server) {
		this.server = server;
		this.timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (++timeOutCount >= CONFIGURATIONS.ROUTING_OPTIMIZATION_TIME_OUT) {
					timeOutCount = 0;
					optimize();
				}
			}
		}, 1000, 1000);
	}

	/**
	 * Initialize routing table. This resets the routing table and calls 
	 * <code>initFingerTable</code> 
	 */
	@Override
	public void init() {
		this.serverId = this.server.getNode().getServerId();
		this.serverIdAsBigInt = new BigInteger(1, this.serverId);
		this.initFingerTable();
	}

	/**
	 * Initialize routing table. The server it self will be set as the 
	 * predecessor, successor and as all the successor entries in the finger table.
	 */
	private void initFingerTable() {
		synchronized (fingerTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				for (int j = 0; j < PASTRY_TABLE_NUMBER_BASE; j++) {
					for (int k = 0; k < NUMBER_OF_ENTRIES; k++) {
						fingerTable[i][j][k] = null;
					}
				}
			}
		}
		/**
		 * initializes by setting this server it self as the predecessor and successor.
		 */
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				predecessors[i] = null;
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				successors[i] = null;
			}
		}
	}

	@Override
	public void triggerOptimize() {
		timeOutCount = CONFIGURATIONS.ROUTING_OPTIMIZATION_TIME_OUT;
	}

	/**
	 * Optimizes the finger table. Sends messages to the each successor in 
	 * the finger table entries to find the optimal successor for those entries.
	 */
	private void optimize() {
		Message msg = new Message(MessageType.DISCOVER_NEIGHBORS, this.server.getNode(), 2);
		msg.setFailedNodeSet(server.getConnectionPool().getFailedNodes());
//		msg.setNeighbourSet(getNeighbourSet());
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
		int raw = getRaw(this.serverId, destination);
		if (raw == -1) {
			return this.server.getNode();
		}
		int column = getColumn(destination, raw);
		synchronized (fingerTable) {
			for (Node node : fingerTable[raw][column]) {
				if (node != null) {
					return node;
				}
			}

			int tColumn, iRaw, iColumn;

			iRaw = raw;
			iColumn = column;
			tColumn = getColumn(this.serverId, raw);
			boolean traverseDown = column < tColumn;
			if (raw == 0) {
				traverseDown = false;
			}
			if (raw == TABLE_SIZE - 1) {
				traverseDown = true;
			}

			while (true) {
				for (Node node : fingerTable[raw][column]) {
					if (node != null) {
						return node;
					}
				}

				tColumn = getColumn(this.serverId, raw);

				if (traverseDown && tColumn == column) {
					raw--;
					traverseDown = raw != 0;
					column = -1;
				} else if (!traverseDown && column == PASTRY_TABLE_NUMBER_BASE - 1) {
					raw++;
					traverseDown = raw >= PastryTable.TABLE_SIZE - 1;
					column = getColumn(this.serverId, raw);
				}

				column = (column + 1) % PASTRY_TABLE_NUMBER_BASE;
				if (iRaw == raw && iColumn == column) {
					throw new RuntimeException("This happens :P");
					//return;
				}
			}
		}
	}

	@Override
	public Set<Node> getNeighbourSet() {
		//initializes an empty node set.
		Set<Node> neighbourSet = new HashSet<Node>();
		synchronized (fingerTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				for (int j = 0; j < PASTRY_TABLE_NUMBER_BASE; j++) {
					for (int k = 0; k < NUMBER_OF_ENTRIES; k++) {
						if (fingerTable[i][j][k] != null) {
							neighbourSet.add(fingerTable[i][j][k]);
						}
					}
				}
			}
		}
		//adds the predecessors & the successors.
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (predecessors[i] != null) {
					neighbourSet.add(this.predecessors[i]);
				}
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (successors[i] != null) {
					neighbourSet.add(this.successors[i]);
				}
			}
		}
		//returns the node set.
		return neighbourSet;
	}

	@Override
	public boolean updateTable(Node node, boolean add) {
		if (Arrays.equals(node.getServerId(), this.serverId)) {
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

	private boolean addNode(Node node) {
		boolean updatedSuccessors = false, updatedPredecessors = false, updatedTable = false;
		Node tempNodeToUpdate = node;
		synchronized (successors) {
			BigInteger successorWRT = ZERO;
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				/**
				 * Checks whether the new node should be set as the successors or not. 
				 * if successors is the server itself (successorOffset == 0) or if the new node 
				 * is not the server it self (newNodeOffset != 0) and it successes 
				 * the server than the existing successors (newNodeOffset < successorOffset), 
				 * the new node will be set as the successors.
				 * 0-----this.server.id------new.server.id-------existing.successors.id----------End.of.Grid
				 */
				BigInteger newNodeOffset = getOffset(tempNodeToUpdate.getServerId());
				if (successorWRT.compareTo(newNodeOffset) == -1
						&& (successors[i] == null
						|| newNodeOffset.compareTo(getOffset(successors[i].getServerId())) == -1)) {
					Node temp = this.successors[i];
					this.successors[i] = tempNodeToUpdate.deepCopy();
					tempNodeToUpdate = temp;
					updatedSuccessors = true;
				}
				if (tempNodeToUpdate == null || successors[i] == null) {
					break;
				}
				successorWRT = getOffset(successors[i].getServerId());
			}
		}
		tempNodeToUpdate = node;
		synchronized (predecessors) {
			BigInteger predecessorWRT = SinchanaServer.GRID_SIZE;
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				/**
				 * Checks whether the new node should be set as the predecessors or not. 
				 * if predecessors is the server itself (predecessorOffset == 0) or if the new node 
				 * is not the server it self (newNodeOffset != 0) and it predecesses 
				 * the server than the existing predecessors (predecessorOffset < newNodeOffset), 
				 * the new node will be set as the predecessors.
				 * 0-----existing.predecessors.id------new.server.id-------this.server.id----------End.of.Grid
				 */
				BigInteger newNodeOffset = getOffset(tempNodeToUpdate.getServerId());
				if (newNodeOffset.compareTo(predecessorWRT) == -1
						&& (predecessors[i] == null || getOffset(predecessors[i].getServerId()).compareTo(newNodeOffset) == -1)) {
					Node temp = this.predecessors[i];
					this.predecessors[i] = tempNodeToUpdate.deepCopy();
					tempNodeToUpdate = temp;
					updatedPredecessors = true;
				}
				if (tempNodeToUpdate == null || predecessors[i] == null) {
					break;
				}
				predecessorWRT = getOffset(predecessors[i].getServerId());
			}
		}
		int raw = getRaw(this.serverId, node.getServerId());
		int column = getColumn(node.getServerId(), raw);
		synchronized (fingerTable) {
			for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
				if (fingerTable[raw][column][i] != null && Arrays.equals(fingerTable[raw][column][i].getServerId(), node.getServerId())) {
					break;
				} else if (fingerTable[raw][column][i] == null) {
					fingerTable[raw][column][i] = node.deepCopy();
					updatedTable = true;
					break;
				}
			}
		}
		return updatedPredecessors || updatedSuccessors || updatedTable;
	}

	private boolean removeNode(Node nodeToRemove) {
		Set<Node> neighbourSet = getNeighbourSet();
		if (neighbourSet.contains(nodeToRemove)) {
			neighbourSet.remove(nodeToRemove);
			//reset the predecessors, successors and finger table entries.
			initFingerTable();
			for (Node node : neighbourSet) {
				addNode(node);
			}
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
		return SinchanaServer.GRID_SIZE.add(new BigInteger(1, id)).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE);
	}

	private int getRaw(byte[] id, byte[] newId) {
		BigInteger t1 = new BigInteger(1, id);
		BigInteger t2 = new BigInteger(1, newId);
		int i = -1;
		while (!t1.equals(t2)) {
			i++;
			t1 = t1.divide(FACTOR);
			t2 = t2.divide(FACTOR);
		}
		return i;
	}

	private int getColumn(byte[] id, int raw) {
		BigInteger val = new BigInteger(1, id);
		val = val.divide(FACTOR.pow(raw));
		val = val.mod(FACTOR);
		return val.intValue();
	}

	private Node checkInLeafSet(byte[] destinationId) {
		BigInteger nodeOffset, destinationOffset = getOffset(destinationId);
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (successors[i] == null) {
					break;
				}
				nodeOffset = getOffset(successors[i].getServerId());
				if (destinationOffset.compareTo(nodeOffset) != 1) {
					return successors[i];
				}
			}
		}
		Node node = this.server.getNode();
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (predecessors[i] == null) {
					break;
				}
				nodeOffset = getOffset(predecessors[i].getServerId());
				if (nodeOffset.compareTo(destinationOffset) == -1) {
					return node;
				}
				node = predecessors[i];
			}
		}
		return null;
	}
}
