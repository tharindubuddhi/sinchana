/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.tapastry;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.logging.Level;
import sinchana.PortHandler;
import sinchana.RoutingHandler;
import sinchana.SinchanaServer;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.Set;
import sinchana.CONFIGURATIONS;

/**
 *
 * @author Hiru
 */
public class TapestryTable implements RoutingHandler, Runnable {

	public static final int TABLE_SIZE = 37;
	private static final int SUCCESSOR_LEVEL = 3;
	private final SinchanaServer server;
	private final Node[] successor = new Node[SUCCESSOR_LEVEL];
	private final Node[] predecessor = new Node[SUCCESSOR_LEVEL];
	private byte[] serverId;
	private BigInteger serverIdAsBigInt;
	private final Node[][] fingerTable = new Node[TABLE_SIZE][10];

	/**
	 * Class constructor with the server instance where the routing table is initialize.
	 * @param server		SinchanaServer instance. The routing table will be initialize based on this.
	 */
	public TapestryTable(SinchanaServer server) {
		this.server = server;
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
		for (int i = 0; i < fingerTable.length; i++) {
			for (int j = 0; j < 10; j++) {
				fingerTable[i][j] = null;
			}
		}
		/**
		 * initializes by setting this server it self as the predecessor and successor.
		 */
		this.predecessor[0] = this.server.deepCopy();
		this.successor[0] = this.server.deepCopy();
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setPredecessor(predecessor[0]);
			this.server.getSinchanaTestInterface().setSuccessor(successor[0]);
//						this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
		}
	}

	/**
	 * Optimizes the finger table. Sends messages to the each successor in 
	 * the finger table entries to find the optimal successor for those entries.
	 */
	@Override
	public void optimize() {
		PortHandler ph = this.server.getPortHandler();
		Message msg;
		for (Node[] nodes : fingerTable) {
			for (Node node : nodes) {
				if (node != null && !Arrays.equals(node.getServerId(), this.serverId)) {
					msg = new Message(this.server, MessageType.DISCOVER_NEIGHBORS, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
					ph.send(msg, node);
				}
			}
		}
	}

	/**
	 * Imports the set of neighbors from the given node.
	 * @param neighbour		Node to import neighbors.
	 */
	private void importNeighbours(Node neighbour) {
		Logger.log(this.server, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 3,
				"Importing neighbour set from " + neighbour.serverId);
		Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBORS, 256);
		this.server.getPortHandler().send(msg, neighbour);
	}

	@Override
	public Node[] getSuccessors() {
		return successor;
	}

	@Override
	public Node[] getPredecessors() {
		return predecessor;
	}

	@Override
	public Node getNextNode(byte[] destination) {

		int raw = getRaw(this.serverId, destination);
		if (raw == -1) {
//						System.out.println(this.serverId + ": next node for " + destination + " is this server.");
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
//								System.out.println(this.serverId + ": found "
//										+ fingerTable[raw][column].serverId + " @ ["
//										+ raw + "," + column + "]");
					break;
				}
			} catch (Exception e) {
				System.out.println(this.serverId + ":\tr:" + raw
						+ "\tc:" + column + "\ttc" + getColumn(this.serverId, raw)
						+ "\tt:" + traverseDown
						+ "---------------------------------------------------------------------------------");
			}
			tColumn = getColumn(this.serverId, raw);
//						System.out.println(this.serverId + ": analize:::\td:" + destination
//								+ "\tr:" + raw + "\tc:" + column + "\ttc:" + tColumn);

			if (traverseDown && tColumn == column) {
//								System.out.println(this.serverId + ": go down");
				raw--;
				traverseDown = raw != 0;
				column = -1;
			} else if (!traverseDown && column == 9) {
//								System.out.println(this.serverId + ": go up");
				raw++;
				traverseDown = raw >= TapestryTable.TABLE_SIZE - 1;
				column = getColumn(this.serverId, raw);
			}

			column = (column + 1) % 10;
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
				/**
				 * add each successor in the finger table to the set if it is
				 * not this server it self. This is because the requester knows 
				 * about this server, so sending it again is a waste.
				 */
				if (node != null && !Arrays.equals(node.getServerId(), this.serverId)) {
					neighbourSet.add(node);
				}
			}

		}
		//adds the predecessor.
		if (this.predecessor != null && !Arrays.equals(this.predecessor[0].getServerId(), this.serverId)) {
			neighbourSet.add(this.predecessor[0]);
		}
		//adds the successor.
		if (this.successor != null && !Arrays.equals(this.successor[0].getServerId(), this.serverId)) {
			neighbourSet.add(this.successor[0]);
		}
		//returns the node set.
		return neighbourSet;
	}

	@Override
	public boolean updateTable(Node node, boolean add) {
		if (Arrays.equals(node.getServerId(), this.serverId)) {
			return false;
		}
		//calculates offsets for the ids.
		BigInteger newNodeOffset = getOffset(node.getServerId());
		boolean updated = false;
		synchronized (this) {
			BigInteger successorOffset = getOffset(this.successor[0].getServerId());
			Logger.log(this.server, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 7,
					"Updating table: S:" + this.successor[0].serverId + " P:" + this.predecessor[0].serverId
					+ " N:" + node.serverId);
			/**
			 * Checks whether the new node should be set as the successor or not. 
			 * if successor is the server itself (successorOffset == 0) or if the new node 
			 * is not the server it self (newNodeOffset != 0) and it successes 
			 * the server than the existing successor (newNodeOffset < successorOffset), 
			 * the new node will be set as the successor.
			 * 0-----this.server.id------new.server.id-------existing.successor.id----------End.of.Grid
			 */
			if (successorOffset.equals(new BigInteger("0", 16))
					|| (!newNodeOffset.equals(new BigInteger("0", 16))
					&& newNodeOffset.compareTo(successorOffset) == -1)) {
				Logger.log(this.server, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 8,
						"Node " + node + " is set as successor overriding " + this.successor[0]);
				this.successor[0] = node.deepCopy();
				updated = true;
			}

			/**
			 * Checks whether the new node should be set as the predecessor or not. 
			 * if predecessor is the server itself (predecessorOffset == 0) or if the new node 
			 * is not the server it self (newNodeOffset != 0) and it predecesses 
			 * the server than the existing predecessor (predecessorOffset < newNodeOffset), 
			 * the new node will be set as the predecessor.
			 * 0-----existing.predecessor.id------new.server.id-------this.server.id----------End.of.Grid
			 */
			BigInteger predecessorOffset = getOffset(this.predecessor[0].getServerId());
			if (predecessorOffset.equals(new BigInteger("0", 16))
					|| (!newNodeOffset.equals(new BigInteger("0", 16))
					&& predecessorOffset.compareTo(newNodeOffset) == -1)) {
				Logger.log(this.server, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 9,
						"Node " + node + " is set as predecessor overriding " + this.predecessor[0]);
				this.predecessor[0] = node.deepCopy();
				updated = true;
			}

			int raw = getRaw(this.serverId, node.getServerId());
			int column = getColumn(node.getServerId(), raw);
			updated = fingerTable[raw][column] == null;
			fingerTable[raw][column] = node.deepCopy();

		}
		/**
		 * if the neighbor set has not imported, imports it from the predecessor.
		 * This will happens only once after the node successfully joined to the grid.
		 */
		return updated;
	}

	@Override
	public void run() {
		while (true) {
			this.optimize();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException ex) {
				java.util.logging.Logger.getLogger(TapestryTable.class.getName()).log(Level.SEVERE, null, ex);
			}
			break;
		}
	}

	/**
	 * Returns the offset of the id relative to the this server.
	 * <code>(id + RoutingHandler.GRID_SIZE - this.server.serverId) % RoutingHandler.GRID_SIZE;</code>
	 * @param id	Id to calculate the offset.
	 * @return		Offset of the id relative to this server.
	 */
	private BigInteger getOffset(byte[] id) {
		return SinchanaServer.GRID_SIZE.add(new BigInteger(1, id)).subtract(server.getServerIdAsBigInt()).mod(SinchanaServer.GRID_SIZE);
	}

	private int getRaw(byte[] id, byte[] newId) {
		BigInteger factor, t1, t2;
		for (int i = TABLE_SIZE - 1; i >= 0; i--) {
			factor = new BigInteger("10", 16).pow(i);
			t1 = new BigInteger(1, id).divide(factor);
			t2 = new BigInteger(1, newId).divide(factor);
			if (!t1.equals(t2)) {
				return i;
			}
		}
		return -1;
	}

	private int getColumn(byte[] id, int raw) {
		return (new BigInteger(1, id).mod(new BigInteger("10", 16).pow(raw + 1))
				.divide(new BigInteger("10", 16).pow(raw))).intValue();
	}
}
