/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.tapastry;

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
public class TapestryTable implements RoutingHandler {

	public static final int TABLE_SIZE = (int) Math.log10(Math.pow(2, 160));
	public static final int NUMBER_BASE = 10;
	private static final int SUCCESSOR_LEVEL = 3;
	public static final BigInteger ZERO = new BigInteger("0", CONFIGURATIONS.NUMBER_BASE);
	private final SinchanaServer server;
	private final Node[] successors = new Node[SUCCESSOR_LEVEL];
	private final Node[] predecessors = new Node[SUCCESSOR_LEVEL];
	private byte[] serverId;
	private BigInteger serverIdAsBigInt;
	private final Node[][] fingerTable = new Node[TABLE_SIZE][NUMBER_BASE];
	private final Timer timer = new Timer();
	private int timeOutCount = 0;

	/**
	 * Class constructor with the server instance where the routing table is initialize.
	 * @param server		SinchanaServer instance. The routing table will be initialize based on this.
	 */
	public TapestryTable(SinchanaServer server) {
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
		this.serverId = this.server.getServerId();
		this.serverIdAsBigInt = new BigInteger(1, this.serverId);
		synchronized (this) {
			this.initFingerTable();
		}
	}

	/**
	 * Initialize routing table. The server it self will be set as the 
	 * predecessor, successor and as all the successor entries in the finger table.
	 */
	private void initFingerTable() {
		for (int i = 0; i < TABLE_SIZE; i++) {
			for (int j = 0; j < NUMBER_BASE; j++) {
				fingerTable[i][j] = null;
			}
		}
		/**
		 * initializes by setting this server it self as the predecessor and successor.
		 */
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				predecessors[i] = this.server.deepCopy();
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				successors[i] = this.server.deepCopy();
			}
		}
	}

	/**
	 * Optimizes the finger table. Sends messages to the each successor in 
	 * the finger table entries to find the optimal successor for those entries.
	 */
	@Override
	public void optimize() {
		Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBORS, 2);
		Set<Node> failedNodes = server.getConnectionPool().getFailedNodes();
		msg.setFailedNodeSet(failedNodes);
		synchronized (predecessors) {
			if (!Arrays.equals(this.serverId, this.predecessors[0].getServerId())) {
				this.server.getPortHandler().send(msg, this.predecessors[0]);
			}
		}
		synchronized (successors) {
			if (!Arrays.equals(this.serverId, this.successors[0].getServerId())) {
				this.server.getPortHandler().send(msg, this.successors[0]);
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

		int raw = getRaw(this.serverId, destination);
		if (raw == -1) {
//			System.out.println(this.serverId + ": next node for " + destination + " is this server.");
			return this.server;
		}
		int column = getColumn(destination, raw);
		if (fingerTable[raw][column] != null) {
			return fingerTable[raw][column];
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
			try {
				if (fingerTable[raw][column] != null) {
//					System.out.println(this.serverId + ": found "
//							+ fingerTable[raw][column].serverId + " @ [" + raw + "," + column + "]");
					break;
				}
			} catch (Exception e) {
				System.out.println(this.serverId + ":\tr:" + raw
						+ "\tc:" + column + "\ttc" + getColumn(this.serverId, raw)
						+ "\tt:" + traverseDown
						+ "---------------------------------------------------------------------------------");
			}
			tColumn = getColumn(this.serverId, raw);
//			System.out.println(this.serverId + ": analize:::\td:" + destination
//								+ "\tr:" + raw + "\tc:" + column + "\ttc:" + tColumn);

			if (traverseDown && tColumn == column) {
//				System.out.println(this.serverId + ": go down");
				raw--;
				traverseDown = raw != 0;
				column = -1;
			} else if (!traverseDown && column == NUMBER_BASE - 1) {
//				System.out.println(this.serverId + ": go up");
				raw++;
				traverseDown = raw >= TapestryTable.TABLE_SIZE - 1;
				column = getColumn(this.serverId, raw);
			}

			column = (column + 1) % NUMBER_BASE;
			if (iRaw == raw && iColumn == column) {
				System.out.println(this.serverId + ": No result found!");
				break;
			}
		}
		return fingerTable[raw][column];
	}

	@Override
	public Set<Node> getNeighbourSet() {
		//initializes an empty node set.
		Set<Node> neighbourSet = new HashSet<Node>();
		for (Node[] nodes : fingerTable) {
			for (Node node : nodes) {
				if (node != null && !Arrays.equals(node.getServerId(), this.serverId)) {
					neighbourSet.add(node);
				}
			}

		}
		//adds the predecessors & the successors.
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (!Arrays.equals(this.predecessors[i].getServerId(), this.serverId)) {
					neighbourSet.add(this.predecessors[i]);
				}
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (!Arrays.equals(this.successors[i].getServerId(), this.serverId)) {
					neighbourSet.add(this.successors[i]);
				}
			}
		}
		//returns the node set.
		return neighbourSet;
	}

	@Override
	public boolean updateTable(Node node, boolean add) {
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
		if (Arrays.equals(node.getServerId(), this.serverId)) {
			return false;
		}
		boolean updated = false;
		//calculates offsets for the ids.
		BigInteger newNodeOffset = getOffset(node.getServerId());

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
				BigInteger successorOffset = getOffset(this.successors[i].getServerId());
				if (successorWRT.compareTo(newNodeOffset) == -1
						&& (newNodeOffset.compareTo(successorOffset) == -1
						|| successorOffset.equals(ZERO))) {
//					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 8,
//							"Node " + node + " is set as successor overriding " + this.successors[i]);
					this.successors[i] = node.deepCopy();
					updated = true;
				}
				if (!Arrays.equals(successors[i].getServerId(), this.serverId)) {
					successorWRT = getOffset(successors[i].getServerId());
				}
			}
		}
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
				BigInteger predecessorOffset = getOffset(this.predecessors[i].getServerId());
				if (newNodeOffset.compareTo(predecessorWRT) == -1
						&& (predecessorOffset.compareTo(newNodeOffset) == -1
						|| predecessorOffset.equals(ZERO))) {
//					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 9,
//							"Node " + node + " is set as predecessor overriding " + this.predecessors[i]);
					this.predecessors[i] = node.deepCopy();
					updated = true;
				}
				if (!Arrays.equals(predecessors[i].getServerId(), this.serverId)) {
					predecessorWRT = getOffset(predecessors[i].getServerId());
				}
			}
		}
		int raw = getRaw(this.serverId, node.getServerId());
		int column = getColumn(node.getServerId(), raw);
		updated = fingerTable[raw][column] == null;
		fingerTable[raw][column] = node.deepCopy();
		return updated;
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

	private BigInteger getOffset(BigInteger id) {
		return SinchanaServer.GRID_SIZE.add(id).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE);
	}
	
	private int getRaw(byte[] id, byte[] newId) {
		BigInteger factor, t1, t2;
		for (int i = TABLE_SIZE - 1; i >= 0; i--) {
			factor = new BigInteger(Integer.toString(NUMBER_BASE)).pow(i);
			t1 = new BigInteger(1, id).divide(factor);
			t2 = new BigInteger(1, newId).divide(factor);
			if (!t1.equals(t2)) {
				return i;
			}
		}
		return -1;
	}

	private int getColumn(byte[] id, int raw) {
		return (new BigInteger(1, id).mod(new BigInteger(Integer.toString(NUMBER_BASE))
				.pow(raw + 1)).divide(new BigInteger(Integer.toString(NUMBER_BASE)).pow(raw))).intValue();
	}
}
