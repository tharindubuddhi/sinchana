/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.chord;

import java.math.BigInteger;
import java.util.HashMap;
import sinchana.RoutingHandler;
import sinchana.Server;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
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
	private final FingerTableEntry[] fingerTable = new FingerTableEntry[TABLE_SIZE];
	private Server server;
	private Node successor = null;
	private Node predecessor = null;
	private String serverId;
	private BigInteger serverIdAsBigInt;
	private Timer timer = new Timer();
	private int timeOutCount = 0;
	private Map<String, Node> failedNodes = new HashMap<String, Node>();

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
		synchronized (this) {
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
			this.initFingerTable();
		}
	}

	/**
	 * Initialize routing table. The server it self will be set as the 
	 * predecessor, successor and as all the successor entries in the finger table.
	 */
	private void initFingerTable() {
		for (int i = 0; i < fingerTable.length; i++) {
			fingerTable[i].setSuccessor(this.server.deepCopy());
		}
		/**
		 * initializes by setting this server it self as the predecessor and successor.
		 */
		this.predecessor = this.server.deepCopy();
		this.successor = this.server.deepCopy();
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setPredecessor(predecessor);
			this.server.getSinchanaTestInterface().setSuccessor(successor);
			this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
		}
	}

	/**
	 * Optimizes the finger table. Sends messages to the each successor in 
	 * the finger table entries to find the optimal successor for those entries.
	 */
	@Override
	public void optimize() {
		synchronized (this) {
			if (!this.serverId.equals(this.predecessor.serverId)) {
				this.importNeighbours(this.predecessor);
			}
			if (!this.serverId.equals(this.successor.serverId)) {
				this.importNeighbours(this.successor);
			}
		}
	}

	/**
	 * Imports the set of neighbors from the given node.
	 * @param neighbour		Node to import neighbors.
	 */
	private void importNeighbours(Node neighbour) {
		Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 3,
				"Importing neighbour set from " + neighbour.serverId);
		Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBOURS, 256);
		msg.setFailedNodeSet(failedNodes);
		this.server.getPortHandler().send(msg, neighbour);
		timeOutCount = 0;
	}

	@Override
	public Node getSuccessor() {
		return successor;
	}

	@Override
	public Node getPredecessor() {
		return predecessor;
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
			 * If so, returns the successor of that entry.
			 * 0-----finger.table.entry.start------destination.id-------finger.table.entry.end----------End.of.Grid
			 */
			if (startOffset.compareTo(destinationOffset) != 1
					&& destinationOffset.compareTo(endOffset) != 1) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 4,
						"Next hop for the destination " + destination
						+ " in " + fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd()
						+ " is " + fingerTableEntry.getSuccessor().serverId);
				return fingerTableEntry.getSuccessor();
			}
		}
		/**
		 * the only id which is not found in the table is the id of this server itself.
		 * Successor of that id is this server. So returns this server.
		 */
		Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 4,
				"Next hop for the destination " + destination + " is this server");
		return this.server;
	}

	@Override
	public Map<String, Node> getNeighbourSet() {
		//initializes an empty node set.
		Map<String, Node> neighbourSet = new HashMap<String, Node>();
		for (FingerTableEntry fingerTableEntry : fingerTable) {
			/**
			 * add each successor in the finger table to the set if it is
			 * not this server it self. This is because the requester knows 
			 * about this server, so sending it again is a waste.
			 */
			if (fingerTableEntry.getSuccessor() != null
					&& !fingerTableEntry.getSuccessor().serverId.equals(this.serverId)) {
				neighbourSet.put(fingerTableEntry.getSuccessor().serverId, fingerTableEntry.getSuccessor());
			}
		}
		//adds the predecessor.
		if (!this.predecessor.serverId.equals(this.serverId)) {
			neighbourSet.put(this.predecessor.serverId, this.predecessor);
		}
		//adds the successor.
		if (!this.successor.serverId.equals(this.serverId)) {
			neighbourSet.put(this.successor.serverId, this.successor);
		}
		//returns the node set.
		return neighbourSet;
	}

	@Override
	public Map<String, Node> getFailedNodeSet() {
		return failedNodes;
	}

	@Override
	public void getOptimalSuccessor(Message message) {
		//calculates offset.
		BigInteger idOffset = getOffset(message.getStartOfRange());
		BigInteger neighBourOffset, mostAdvance = new BigInteger("0", 16);
		//initializes to this server.
		Node optimalSuccessor = this.server;
		//get the neighbor set (the knows node set of this server)
		Map<String, Node> neighbourSet = getNeighbourSet();
		//considering each node
		Set<String> keySet = neighbourSet.keySet();
		for (String nodeId : keySet) {
			neighBourOffset = getOffset(nodeId);
			/**
			 * checks if the node successes the id (idOffset <= neighBourOffset)
			 * and is it much suitable than the existing one (mostAdvance < neighBourOffset)
			 */
			if (idOffset.compareTo(neighBourOffset) != 1
					&& mostAdvance.compareTo(neighBourOffset) == -1) {
				//if so, sets it as the most suitable one.
				mostAdvance = neighBourOffset;
				optimalSuccessor = neighbourSet.get(nodeId);
			}
		}


		if (optimalSuccessor.serverId.equals(this.server.serverId)) {
			if (!message.station.serverId.equals(message.source.serverId)) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 6,
						"Valid response for " + message.targetKey
						+ " in " + message.startOfRange + "-" + message.endOfRange
						+ " is found as " + optimalSuccessor.serverId
						+ " where source is " + message.source.serverId
						+ " & prevstation is " + message.station.serverId);
				message.setSuccessor(this.server.deepCopy());
				this.server.getPortHandler().send(message, message.source);
			}
		} else {
			this.server.getPortHandler().send(message, optimalSuccessor);
		}
	}

	@Override
	public void updateTable(Node node) {
		if (failedNodes.remove(node.serverId) != null) {
			System.out.println("Ok, it hits here :P ::: " + node);
		}

		//calculates offsets for the ids.
		BigInteger newNodeOffset = getOffset(node.serverId);
		boolean tableChanged = false;
		boolean updated = false;
		synchronized (this) {
			BigInteger successorOffset = getOffset(this.successor.serverId);
			Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 7,
					"Updating table: S:" + this.successor.serverId + " P:" + this.predecessor.serverId
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
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 8,
						"Node " + node + " is set as successor overriding " + this.successor);
				this.successor = node.deepCopy();
				updated = true;
				if (this.server.getSinchanaTestInterface() != null) {
					this.server.getSinchanaTestInterface().setSuccessor(successor);
				}
			}

			/**
			 * Checks whether the new node should be set as the predecessor or not. 
			 * if predecessor is the server itself (predecessorOffset == 0) or if the new node 
			 * is not the server it self (newNodeOffset != 0) and it predecesses 
			 * the server than the existing predecessor (predecessorOffset < newNodeOffset), 
			 * the new node will be set as the predecessor.
			 * 0-----existing.predecessor.id------new.server.id-------this.server.id----------End.of.Grid
			 */
			BigInteger predecessorOffset = getOffset(this.predecessor.serverId);
			if (predecessorOffset.equals(new BigInteger("0", 16))
					|| (!newNodeOffset.equals(new BigInteger("0", 16))
					&& predecessorOffset.compareTo(newNodeOffset) == -1)) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 9,
						"Node " + node + " is set as predecessor overriding " + this.predecessor);
				this.predecessor = node.deepCopy();
				updated = true;
				if (this.server.getSinchanaTestInterface() != null) {
					this.server.getSinchanaTestInterface().setPredecessor(predecessor);
				}
			}

			BigInteger startOffset;
			for (int i = 0; i < fingerTable.length; i++) {
				startOffset = getOffset(fingerTable[i].getStart());
				successorOffset = getOffset(fingerTable[i].getSuccessor().serverId);

				/**
				 * Checks whether the new node should be set as the successor or not. 
				 * if the new node successes the table entry (startOffset <= newNodeOffset) and
				 * if it is a much suitable successor than the existing one 
				 * (newNodeOffset < successorOffset || successorOffset == 0), 
				 * the new node will be set as the successor for that fingertable entry.
				 * 0-----finger.table.entry.start------new.server.id-------existing.successor.id----------End.of.Grid
				 */
				if (startOffset.compareTo(newNodeOffset) != 1
						&& (newNodeOffset.compareTo(successorOffset) == -1
						|| successorOffset.equals(new BigInteger("0", 16)))) {
					Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 0,
							"Setting " + node.serverId + " as successor of "
							+ fingerTable[i].getStart() + "-" + fingerTable[i].getEnd()
							+ " overriding " + fingerTable[i].getSuccessor().serverId);
					fingerTable[i].setSuccessor(node.deepCopy());
					updated = true;
					tableChanged = true;
				}
			}
		}
		/**
		 * updates the testing interfaces if the table is altered.
		 */
		if (tableChanged && this.server.getSinchanaTestInterface() != null) {
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
		synchronized (this) {
			failedNodes.put(nodeToRemove.serverId, nodeToRemove.deepCopy());
			//gets the known node set.
			Map<String, Node> neighbourSet = getNeighbourSet();
			if (neighbourSet.containsKey(nodeToRemove.serverId)) {
				neighbourSet.remove(nodeToRemove.serverId);
				Set<String> keySet = neighbourSet.keySet();
				//reset the predecessor, successor and finger table entries.

				initFingerTable();
				for (String nodeId : keySet) {
					updateTable(neighbourSet.get(nodeId));
				}
			}
		}
	}

	@Override
	public void removeNode(Map<String, Node> nodesToRemove) {
		synchronized (this) {
			failedNodes.putAll(nodesToRemove);
			Set<String> keySet = nodesToRemove.keySet();
			boolean needToUpdate = false;
			//gets the known node set.
			Map<String, Node> neighbourSet = getNeighbourSet();
			for (String nodeId : keySet) {
				if (neighbourSet.containsKey(nodeId)) {
					neighbourSet.remove(nodeId);
					needToUpdate = true;
				}
			}
			if (needToUpdate) {
				//reset the predecessor, successor and finger table entries.
				initFingerTable();
				Set<String> neighbourSetIds = neighbourSet.keySet();
				for (String nodeId : neighbourSetIds) {
					updateTable(neighbourSet.get(nodeId));
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
}
