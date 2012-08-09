/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.chord;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import sinchana.RoutingHandler;
import sinchana.Server;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import sinchana.CONFIGURATIONS;

/**
 *
 * @author Hiru
 */
public class ChordTable implements RoutingHandler {

	public static final int TABLE_SIZE = 160;
	public static final int SUCCESSOR_LEVEL = 3;
	private final FingerTableEntry[] fingerTable = new FingerTableEntry[TABLE_SIZE];
	private final Server server;
	private final Node[] successors = new Node[SUCCESSOR_LEVEL];
	private final Node[] predecessors = new Node[SUCCESSOR_LEVEL];
	private String serverId;
	private BigInteger serverIdAsBigInt;
	private final Timer timer = new Timer();
	private int timeOutCount = 0;
	private final Map<String, NodeInfoContainer> failedNodes = new HashMap<String, NodeInfoContainer>();

	/**
	 * Class constructor with the server instance where the routing table is initialize.
	 * @param server		Server instance. The routing table will be initialize based on this.
	 */
	public ChordTable(Server server) {
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
		this.serverIdAsBigInt = new BigInteger(this.serverId, 16);
		synchronized (fingerTable) {
			for (int i = 0; i < fingerTable.length; i++) {
				/**
				 * calculates start and end of the finger table entries in 2's power.
				 * initializes the table by setting this server as the successors for
				 * all the table entries.
				 */
				fingerTable[i] = new FingerTableEntry();
				fingerTable[i].setStart(new BigInteger("2", 16).pow(i).add(serverIdAsBigInt).mod(Server.GRID_SIZE));
				fingerTable[i].setEnd(new BigInteger("2", 16).pow(i + 1).add(serverIdAsBigInt).subtract(new BigInteger("1", 16)).mod(Server.GRID_SIZE));
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
				fingerTable[i].setSuccessor(this.server.deepCopy());
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
		synchronized (predecessors) {
			if (!this.serverId.equals(this.predecessors[0].serverId)) {
				this.importNeighbours(this.predecessors[0]);
			}
		}
		synchronized (successors) {
			if (!this.serverId.equals(this.successors[0].serverId)) {
				this.importNeighbours(this.successors[0]);
			}
		}
	}

	/**
	 * Imports the set of neighbors from the given node.
	 * @param neighbour		Node to import neighbors.
	 */
	private void importNeighbours(Node neighbour) {
//		Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 3,
//				"Importing neighbour set from " + neighbour.serverId);
		Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBORS, 256);
		msg.setFailedNodeSet(getFailedNodeSet());
		this.server.getPortHandler().send(msg, neighbour);
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
	public Node getNextNode(String destination) {
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
		return this.server;
	}

	@Override
	public Set<Node> getNeighbourSet() {
		//initializes an empty node set.
		Set<Node> neighbourSet = new HashSet<Node>();
		for (FingerTableEntry fingerTableEntry : fingerTable) {
			/**
			 * add each successors in the finger table to the set if it is
			 * not this server it self. This is because the requester knows 
			 * about this server, so sending it again is a waste.
			 */
			if (fingerTableEntry.getSuccessor() != null
					&& !fingerTableEntry.getSuccessor().serverId.equals(this.serverId)) {
				neighbourSet.add(fingerTableEntry.getSuccessor());
			}
		}
		//adds the predecessors & the successors.
		for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
			if (!this.predecessors[i].serverId.equals(this.serverId)) {
				neighbourSet.add(this.predecessors[i]);
			}
			if (!this.successors[i].serverId.equals(this.serverId)) {
				neighbourSet.add(this.successors[i]);
			}
		}
		return neighbourSet;
	}

	@Override
	public Set<Node> getFailedNodeSet() {
		Set<Node> nodes = new HashSet<Node>();
		synchronized (failedNodes) {
			Set<String> keySet = failedNodes.keySet();
			for (String nid : keySet) {
				nodes.add(failedNodes.get(nid).node);
			}
		}
		return nodes;
	}

	@Override
	public void getOptimalSuccessor(Message message) {
		//calculates offset.
		BigInteger idOffset = getOffset(message.getStartOfRange());
		BigInteger neighBourOffset, mostAdvance = new BigInteger("0", 16);
		//initializes to this server.
		Node optimalSuccessor = this.server;
		//get the neighbor set (the knows node set of this server)
		Set<Node> neighbourSet = getNeighbourSet();
		//considering each node
		for (Node node : neighbourSet) {
			neighBourOffset = getOffset(node.serverId);
			/**
			 * checks if the node successes the id (idOffset <= neighBourOffset)
			 * and is it much suitable than the existing one (mostAdvance < neighBourOffset)
			 */
			if (idOffset.compareTo(neighBourOffset) != 1
					&& mostAdvance.compareTo(neighBourOffset) == -1) {
				//if so, sets it as the most suitable one.
				mostAdvance = neighBourOffset;
				optimalSuccessor = node;
			}
		}


		if (optimalSuccessor.serverId.equals(this.server.serverId)) {
			if (!message.station.serverId.equals(message.source.serverId)) {
//				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 6,
//						"Valid response for " + message.targetKey
//						+ " in " + message.startOfRange + "-" + message.endOfRange
//						+ " is found as " + optimalSuccessor.serverId
//						+ " where source is " + message.source.serverId
//						+ " & prevstation is " + message.station.serverId);
				message.setSuccessor(this.server.deepCopy());
				this.server.getPortHandler().send(message, message.source);
			}
		} else {
			this.server.getPortHandler().send(message, optimalSuccessor);
		}
	}

	@Override
	public void updateTable(Node node, boolean ignorePrevFailures) {
		if (node.serverId.equals(this.serverId)) {
			return;
		}
		boolean tableChanged = false;
		boolean updated = false;
		ignorePrevFailures = false;
		synchronized (failedNodes) {
			if (failedNodes.containsKey(node.serverId)) {
				if (ignorePrevFailures || (failedNodes.get(node.serverId).time + CONFIGURATIONS.FAILED_REACCEPT_TIME_OUT)
						< Calendar.getInstance().getTimeInMillis()) {
					failedNodes.remove(node.serverId);
					System.out.println("Adding back " + node);
				} else {
					System.out.println("Not accepted till "
							+ (failedNodes.get(node.serverId).time + CONFIGURATIONS.FAILED_REACCEPT_TIME_OUT - Calendar.getInstance().getTimeInMillis())
							+ "ms -- " + node);
					return;
				}
			}
		}

		//calculates offsets for the ids.
		BigInteger newNodeOffset = getOffset(node.serverId);

		synchronized (successors) {
			BigInteger successorWRT = new BigInteger("0");
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				/**
				 * Checks whether the new node should be set as the successors or not. 
				 * if successors is the server itself (successorOffset == 0) or if the new node 
				 * is not the server it self (newNodeOffset != 0) and it successes 
				 * the server than the existing successors (newNodeOffset < successorOffset), 
				 * the new node will be set as the successors.
				 * 0-----this.server.id------new.server.id-------existing.successors.id----------End.of.Grid
				 */
				BigInteger successorOffset = getOffset(this.successors[i].serverId);
				if (successorWRT.compareTo(newNodeOffset) == -1
						&& (newNodeOffset.compareTo(successorOffset) == -1
						|| successorOffset.equals(new BigInteger("0", 16)))) {
//					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 8,
//							"Node " + node + " is set as successor overriding " + this.successors[i]);
					this.successors[i] = node.deepCopy();
					updated = true;
					if (this.server.getSinchanaTestInterface() != null) {
//						this.server.getSinchanaTestInterface().setSuccessor(successors);
					}
				}
				if (!successors[i].serverId.equals(this.serverId)) {
					successorWRT = getOffset(successors[i].serverId);
				}
			}
		}
		synchronized (predecessors) {
			BigInteger predecessorWRT = Server.GRID_SIZE;
			for (int i = 0; i < SUCCESSOR_LEVEL; i++) {
				/**
				 * Checks whether the new node should be set as the predecessors or not. 
				 * if predecessors is the server itself (predecessorOffset == 0) or if the new node 
				 * is not the server it self (newNodeOffset != 0) and it predecesses 
				 * the server than the existing predecessors (predecessorOffset < newNodeOffset), 
				 * the new node will be set as the predecessors.
				 * 0-----existing.predecessors.id------new.server.id-------this.server.id----------End.of.Grid
				 */
				BigInteger predecessorOffset = getOffset(this.predecessors[i].serverId);
				if (newNodeOffset.compareTo(predecessorWRT) == -1
						&& (predecessorOffset.compareTo(newNodeOffset) == -1
						|| predecessorOffset.equals(new BigInteger("0", 16)))) {
//					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 9,
//							"Node " + node + " is set as predecessor overriding " + this.predecessors[i]);
					this.predecessors[i] = node.deepCopy();
					updated = true;
					if (this.server.getSinchanaTestInterface() != null) {
//						this.server.getSinchanaTestInterface().setPredecessor(predecessors);
					}
				}
				if (!predecessors[i].serverId.equals(this.serverId)) {
					predecessorWRT = getOffset(predecessors[i].serverId);
				}
			}
		}
		synchronized (fingerTable) {
			BigInteger startOffset, successorOffset;
			for (int i = 0; i < fingerTable.length; i++) {
				startOffset = getOffset(fingerTable[i].getStart());
				successorOffset = getOffset(fingerTable[i].getSuccessor().serverId);

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
						|| successorOffset.equals(new BigInteger("0", 16)))) {
//					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 0,
//							"Setting " + node.serverId + " as successor of "
//							+ fingerTable[i].getStart() + "-" + fingerTable[i].getEnd()
//							+ " overriding " + fingerTable[i].getSuccessor().serverId);
					fingerTable[i].setSuccessor(node.deepCopy());
					updated = true;
					tableChanged = true;
				}
			}
		}


		/**
		 * updates the testing interfaces if the table is altered.
		 */
		if (tableChanged
				&& this.server.getSinchanaTestInterface()
				!= null) {
			this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
		}

		if (updated) {
			importNeighbours(node);
		}
	}

	/**
	 * Remove the node from the routing table, temporary update the table and 
	 * send messages to the nodes in the routing table to get the optimal neighbors.
	 * 
	 * @param nodeToRemove Node to remove from the finger table. 
	 */
	@Override
	public void removeNode(Node nodeToRemove) {
		synchronized (failedNodes) {
			NodeInfoContainer nic = new NodeInfoContainer();
			nic.node = nodeToRemove.deepCopy();
			nic.time = Calendar.getInstance().getTimeInMillis();
			failedNodes.put(nodeToRemove.serverId, nic);
		}
		synchronized (fingerTable) {
			//gets the known node set.
			Set<Node> neighbourSet = getNeighbourSet();
			if (neighbourSet.contains(nodeToRemove)) {
				neighbourSet.remove(nodeToRemove);
				//reset the predecessors, successors and finger table entries.

				initFingerTable();
				for (Node node : neighbourSet) {
					updateTable(node, false);
				}
			}
		}
	}

	@Override
	public void removeNode(Set<Node> nodesToRemove) {
		synchronized (this) {
			long time = Calendar.getInstance().getTimeInMillis();
			for (Node node : nodesToRemove) {
				if (!failedNodes.containsKey(node.serverId)) {
					NodeInfoContainer nic = new NodeInfoContainer();
					nic.node = node.deepCopy();
					nic.time = time;
					failedNodes.put(node.serverId, nic);
				}
			}
			boolean needToUpdate = false;
			//gets the known node set.
			Set<Node> neighbourSet = getNeighbourSet();
			for (Node node : nodesToRemove) {
				if (neighbourSet.contains(node)) {
					neighbourSet.remove(node);
					needToUpdate = true;
				}
			}
			if (needToUpdate) {
				//reset the predecessors, successors and finger table entries.
				initFingerTable();
				for (Node node : neighbourSet) {
					updateTable(node, false);
				}
			}
		}
	}

	/**
	 * Returns the offset of the id relative to the this server.
	 * <code>(id + RoutingHandler.GRID_SIZE - this.server.serverId) % RoutingHandler.GRID_SIZE;</code>
	 * @param id	Id to calculate the offset.
	 * @return		Offset of the id relative to this server.
	 */
	private BigInteger getOffset(String id) {
		return Server.GRID_SIZE.add(new BigInteger(id, 16)).subtract(server.getServerIdAsBigInt()).mod(Server.GRID_SIZE);
	}

	private BigInteger getOffset(BigInteger id) {
		return Server.GRID_SIZE.add(id).subtract(server.getServerIdAsBigInt()).mod(Server.GRID_SIZE);
	}

	private class NodeInfoContainer {

		Node node;
		long time;
	}
}
