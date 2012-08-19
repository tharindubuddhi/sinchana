/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.chord;

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
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class ChordTable implements RoutingHandler {

	public static final BigInteger ZERO = new BigInteger("0", CONFIGURATIONS.NUMBER_BASE);
	public static final int TABLE_SIZE = 160;
	public static final int SUCCESSOR_LEVEL = 3;
	private final FingerTableEntry[] fingerTable = new FingerTableEntry[TABLE_SIZE];
	private final SinchanaServer server;
	private final Node[] successors = new Node[SUCCESSOR_LEVEL];
	private final Node[] predecessors = new Node[SUCCESSOR_LEVEL];
	private byte[] serverId;
	private BigInteger serverIdAsBigInt;
	private final Timer timer = new Timer();
	private int timeOutCount = 0;

	/**
	 * Class constructor with the server instance where the routing table is initialize.
	 * @param server		SinchanaServer instance. The routing table will be initialize based on this.
	 */
	public ChordTable(SinchanaServer server) {
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
		this.serverIdAsBigInt = new BigInteger(1, serverId);
		synchronized (fingerTable) {
			for (int i = 0; i < fingerTable.length; i++) {
				/**
				 * calculates start and end of the finger table entries in 2's power.
				 * initializes the table by setting this server as the successors for
				 * all the table entries.
				 */
				fingerTable[i] = new FingerTableEntry();
				fingerTable[i].setStart(new BigInteger("2", 16).pow(i).add(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE));
				fingerTable[i].setEnd(new BigInteger("2", 16).pow(i + 1).add(serverIdAsBigInt).subtract(new BigInteger("1", 16)).mod(SinchanaServer.GRID_SIZE));
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
				fingerTable[i].setSuccessor(this.server.getNode().deepCopy());
			}
		}
		/**
		 * initializes by setting this server it self as the predecessors and successors.
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
	 * Optimizes the finger table. Sends messages to the each successors in 
	 * the finger table entries to find the optimal successors for those entries.
	 */
	private void optimize() {
		Message msg = new Message(MessageType.DISCOVER_NEIGHBORS, this.server.getNode(), 2);
		msg.setFailedNodeSet(server.getConnectionPool().getFailedNodes());
		msg.setNeighbourSet(getNeighbourSet());
		synchronized (predecessors) {
			for (Node node : predecessors) {
				if (node != null) {
					this.server.getIOHandler().send(msg.deepCopy(), node);
				}
			}
		}
		synchronized (successors) {
			for (Node node : successors) {
				if (node != null) {
					this.server.getIOHandler().send(msg.deepCopy(), node);
				}
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

		BigInteger destinationOffset, startOffset, endOffset;
		//calculates the offset to the destination.
		destinationOffset = getOffset(destination);
		//takes finger table entries one by one.
		for (FingerTableEntry fingerTableEntry : fingerTable) {
			startOffset = getOffset(fingerTableEntry.getStart());
			endOffset = getOffset(fingerTableEntry.getEnd());
			/**
			 * checks whether the destination lies within the start and end of the table entry.
			 * If so, returns the successors of that entry.
			 * 0-----finger.table.entry.start------destination.id-------finger.table.entry.end----------End.of.Grid
			 */
			if (startOffset.compareTo(destinationOffset) != 1
					&& destinationOffset.compareTo(endOffset) != 1) {
//				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 4,
//						"Next hop for the destination " + destination
//						+ " in " + fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd()
//						+ " is " + fingerTableEntry.getSuccessor().serverId);
				return fingerTableEntry.getSuccessor();
			}
		}
		/**
		 * the only id which is not found in the table is the id of this server itself.
		 * Successor of that id is this server. So returns this server.
		 */
//		Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 4,
//				"Next hop for the destination " + destination + " is this server");
		return this.server.getNode();
	}

	@Override
	public Set<Node> getNeighbourSet() {
		//initializes an empty node set.
		Set<Node> neighbourSet = new HashSet<Node>();
		synchronized (fingerTable) {
			for (FingerTableEntry fingerTableEntry : fingerTable) {
				/**
				 * add each successors in the finger table to the set if it is
				 * not this server it self. This is because the requester knows 
				 * about this server, so sending it again is a waste.
				 */
				if (fingerTableEntry.getSuccessor() != null
						&& !Arrays.equals(fingerTableEntry.getSuccessor().getServerId(), this.serverId)) {
					neighbourSet.add(fingerTableEntry.getSuccessor());
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
		synchronized (fingerTable) {
			BigInteger startOffset, successorOffset;
			BigInteger newNodeOffset = getOffset(node.getServerId());
			for (int i = 0; i < fingerTable.length; i++) {
				startOffset = getOffset(fingerTable[i].getStart());
				successorOffset = getOffset(fingerTable[i].getSuccessor().getServerId());

				/**
				 * Checks whether the new node should be set as the successors or not. 
				 * if the new node successes the table entry (startOffset <= newNodeOffset) and
				 * if it is a much suitable successors than the existing one 
				 * (newNodeOffset < successorOffset || successorOffset == 0), 
				 * the new node will be set as the successors for that fingertable entry.
				 * 0-----finger.table.entry.start------new.server.id-------existing.successors.id----------End.of.Grid
				 */
				if (startOffset.compareTo(newNodeOffset) != 1
						&& (newNodeOffset.compareTo(successorOffset) == -1
						|| successorOffset.equals(ZERO))) {
//					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 0,
//							"Setting " + node.serverId + " as successor of "
//							+ fingerTable[i].getStart() + "-" + fingerTable[i].getEnd()
//							+ " overriding " + fingerTable[i].getSuccessor().serverId);
					fingerTable[i].setSuccessor(node.deepCopy());
					updatedTable = true;
				}
			}
		}
		if (updatedPredecessors && predecessors[SUCCESSOR_LEVEL - 1] != null) {
			System.out.println(this.server.getServerIdAsString() + " p: ");
			for (Node n : predecessors) {
				System.out.print(" " + (n == null ? "n/a" : ByteArrays.toReadableString(n.serverId)));
			}
			System.out.println("");
		}
		if (updatedSuccessors && successors[SUCCESSOR_LEVEL - 1] != null) {
			System.out.println(this.server.getServerIdAsString() + " s: ");
			for (Node n : successors) {
				System.out.print(" " + (n == null ? "n/a" : ByteArrays.toReadableString(n.serverId)));
			}
			System.out.println("");
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

	private BigInteger getOffset(BigInteger id) {
		return SinchanaServer.GRID_SIZE.add(id).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE);
	}

	private BigInteger getOffset(Node node) {
		if (node == null) {
			return ZERO;
		}
		return SinchanaServer.GRID_SIZE.add(new BigInteger(1, node.getServerId())).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE);
	}
}
