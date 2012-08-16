/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.pastry;

import java.math.BigInteger;
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
 * @author DELL
 */
public class PastryTable implements RoutingHandler{

    public static final int IDSPACE = 160;
    public static final int BASE = 16;
    public static final int TABLE_SIZE = (int) (Math.log(Math.pow(2, IDSPACE))/Math.log(BASE));
	private static final int SUCCESSOR_LEVEL = 3;
	private final SinchanaServer server;
	private byte[] serverId;
	private BigInteger serverIdAsBigInt;
	private final Node[][] fingerTable = new Node[TABLE_SIZE][BASE];
	public static final BigInteger ZERO = new BigInteger("0", CONFIGURATIONS.NUMBER_BASE);
	private final Node[] successors = new Node[SUCCESSOR_LEVEL];
	private final Node[] predecessors = new Node[SUCCESSOR_LEVEL];
	private final Timer timer = new Timer();
	private int timeOutCount = 0;

    
	/**
	 * Class constructor with the server instance where the routing table is initialize.
	 * @param server		SinchanaServer instance. The routing table will be initialize based on this.
	 */
	public PastryTable(SinchanaServer svr) {
		this.server = svr;
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
		this.serverIdAsBigInt = new BigInteger(this.serverId);
		synchronized (fingerTable) {
			for (int i = 0; i < fingerTable.length; i++) {
				/**
				 * calculates start and end of the finger table entries in 2's power.
				 * initializes the table by setting this server as the successors for
				 * all the table entries.
				 */
//				fingerTable[i] = new FingerTableEntry();
//				fingerTable[i].setStart(new BigInteger("2", 16).pow(i).add(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE));
//				fingerTable[i].setEnd(new BigInteger("2", 16).pow(i + 1).add(serverIdAsBigInt).subtract(new BigInteger("1", 16)).mod(SinchanaServer.GRID_SIZE));
			}
		}
		this.initFingerTable();
	}

	/**
	 * Initialize routing table. The server it self will be set as the 
	 * predecessors, successors and as all the successors entries in the finger table.
	 */
	private void initFingerTable() {
		synchronized (fingerTable) {
			for (int i = 0; i < fingerTable.length; i++) {
//				fingerTable[i].setSuccessor(this.server.deepCopy());
			}
		}
		/**
		 * initializes by setting this server it self as the predecessors and successors.
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
	 * Optimizes the finger table. Sends messages to the each successors in 
	 * the finger table entries to find the optimal successors for those entries.
	 */
	@Override
	public void optimize() {
		Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBORS, 2);
		Set<Node> failedNodes = server.getConnectionPool().getFailedNodes();
		msg.setFailedNodeSet(failedNodes);
		synchronized (predecessors) {
			if (!this.serverId.equals(this.predecessors[0].serverId)) {
				this.server.getPortHandler().send(msg, this.predecessors[0]);
			}
		}
		synchronized (successors) {
			if (!this.serverId.equals(this.successors[0].serverId)) {
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
		BigInteger destinationOffset;
		//calculates the offset to the destination.
		destinationOffset = getOffset(destination);
		//takes finger table entries one by one.
		
		return this.server;
	}

	@Override
	public Set<Node> getNeighbourSet() {
		//initializes an empty node set.
		Set<Node> neighbourSet = new HashSet<Node>();
		synchronized (fingerTable) {
//			for (FingerTableEntry fingerTableEntry : fingerTable) {
//				/**
//				 * add each successors in the finger table to the set if it is
//				 * not this server it self. This is because the requester knows 
//				 * about this server, so sending it again is a waste.
//				 */
//				if (fingerTableEntry.getSuccessor() != null
//						&& !fingerTableEntry.getSuccessor().serverId.equals(this.serverId)) {
//					neighbourSet.add(fingerTableEntry.getSuccessor());
//				}
//			}
		}
		//adds the predecessors & the successors.
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (!this.predecessors[i].serverId.equals(this.serverId)) {
					neighbourSet.add(this.predecessors[i]);
				}
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				if (!this.successors[i].serverId.equals(this.serverId)) {
					neighbourSet.add(this.successors[i]);
				}
			}
		}
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
		if (node.serverId.equals(this.serverId)) {
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
				if (!successors[i].serverId.equals(this.serverId)) {
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
				if (!predecessors[i].serverId.equals(this.serverId)) {
					predecessorWRT = getOffset(predecessors[i].getServerId());
				}
			}
		}
		synchronized (fingerTable) {
//			BigInteger startOffset, successorOffset;
//			for (int i = 0; i < fingerTable.length; i++) {
//				startOffset = getOffset(fingerTable[i].getStart());
//				successorOffset = getOffset(fingerTable[i].getSuccessor().serverId);
//
//				/**
//				 * Checks whether the new node should be set as the successors or not. 
//				 * if the new node successes the table entry (startOffset <= newNodeOffset) and
//				 * if it is a much suitable successors than the existing one 
//				 * (newNodeOffset < successorOffset || successorOffset == 0), 
//				 * the new node will be set as the successors for that fingertable entry.
//				 * 0-----finger.table.entry.start------new.server.id-------existing.successors.id----------End.of.Grid
//				 */
//				if (startOffset.compareTo(newNodeOffset) != 1
//						&& (newNodeOffset.compareTo(successorOffset) == -1
//						|| successorOffset.equals(ZERO))) {
////					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 0,
////							"Setting " + node.serverId + " as successor of "
////							+ fingerTable[i].getStart() + "-" + fingerTable[i].getEnd()
////							+ " overriding " + fingerTable[i].getSuccessor().serverId);
//					fingerTable[i].setSuccessor(node.deepCopy());
//					updated = true;
//				}
//			}
		}
		return updated;
	}

	/**
	 * Remove the node from the routing table, temporary update the table and 
	 * send messages to the nodes in the routing table to get the optimal neighbors.
	 * 
	 * @param nodeToRemove Node to remove from the finger table. 
	 */
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
		return SinchanaServer.GRID_SIZE.add(new BigInteger(id)).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE);
	}

	private BigInteger getOffset(BigInteger id) {
		return SinchanaServer.GRID_SIZE.add(id).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE);
	}
}

