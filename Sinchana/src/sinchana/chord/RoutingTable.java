/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.chord;

import sinchana.PortHandler;
import sinchana.RoutingHandler;
import sinchana.Server;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author Hiru
 */
public class RoutingTable implements RoutingHandler {

		private Server server;
		private Node successor = null;
		private Node predecessor = null;
		private int serverId;
		private FingerTableEntry[] fingerTable = new FingerTableEntry[TABLE_SIZE];
		private boolean stable = false;
		private boolean neighboursImported = false;

		public RoutingTable(Server server) {
				this.server = server;
		}

		@Override
		public void init() {
				this.serverId = server.getServerId();
				this.initFingerTable();
		}

		private void initFingerTable() {
				for (int i = 0; i < fingerTable.length; i++) {
						fingerTable[i] = new FingerTableEntry();
						fingerTable[i].setStart((this.serverId + (int) Math.pow(2, i)) % GRID_SIZE);
						fingerTable[i].setEnd((this.serverId + (int) Math.pow(2, i + 1) - 1) % GRID_SIZE);
						fingerTable[i].setSuccessor(null);
				}
				if (this.server.getSinchanaTestInterface() != null) {
						this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
				}
		}

		private synchronized void updateTableWithNode(Node node) {
//				if (Thread.currentThread().getId() != this.server.threadId) {
//						throw new RuntimeException("Invalid thread access! "
//								+ Thread.currentThread().getId() + ": " + Thread.currentThread().getName());
//				}

				int start, successorId, tempNodeId;
				boolean tableChanged = false;
				tempNodeId = (node.serverId + GRID_SIZE - this.serverId) % GRID_SIZE;
				for (int i = 0; i < fingerTable.length; i++) {
						if (fingerTable[i].getSuccessor() != null) {
								start = (fingerTable[i].getStart() + GRID_SIZE - this.serverId) % GRID_SIZE;
								successorId = (fingerTable[i].getSuccessor().serverId + GRID_SIZE - this.serverId) % GRID_SIZE;
								if (start <= tempNodeId && (tempNodeId < successorId || successorId == 0)) {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 0,
												"Setting " + node.serverId + " as successor of "
												+ fingerTable[i].getStart() + "-" + fingerTable[i].getEnd()
												+ " overriding " + fingerTable[i].getSuccessor().serverId);
										fingerTable[i].setSuccessor(node.deepCopy());
										tableChanged = true;
								}
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 2,
										"Setting " + node.serverId + " as successor of "
										+ fingerTable[i].getStart() + "-" + fingerTable[i].getEnd()
										+ " since no other node present");
								fingerTable[i].setSuccessor(node.deepCopy());
								tableChanged = true;
						}
				}
				if (tableChanged && this.server.getSinchanaTestInterface() != null) {
						this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
				}
		}

		private void importNeighbours(Node neighbour) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 3,
						"Importing neighbour set from " + neighbour.serverId);
				Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBOURS, 256);
				this.server.getPortHandler().send(msg, neighbour.address, neighbour.portId);
		}

		private void optimizeFingerTable() {
				PortHandler ph = this.server.getPortHandler();
				Message msg;
				for (FingerTableEntry fingerTableEntry : fingerTable) {
						if (fingerTableEntry.getSuccessor().serverId != this.serverId) {
								msg = new Message(this.server, MessageType.FIND_SUCCESSOR, 256);
								msg.setStartOfRange(fingerTableEntry.getStart());
								msg.setEndOfRange(fingerTableEntry.getEnd());
								ph.send(msg, fingerTableEntry.getSuccessor().address, fingerTableEntry.getSuccessor().portId);
						}
				}
		}

		/*****************************************************************************************************************************/
		@Override
		public Node getSuccessor() {
				return successor;
		}

		@Override
		public Node getPredecessor() {
				return predecessor;
		}

		@Override
		public Node getNextNode(int destination) {
				int destinationOffset, startOffset, endOffset;
				destinationOffset = (destination + RoutingHandler.GRID_SIZE - this.serverId) % RoutingHandler.GRID_SIZE;
				for (FingerTableEntry fingerTableEntry : fingerTable) {
						startOffset = (fingerTableEntry.getStart() + RoutingHandler.GRID_SIZE - this.serverId) % RoutingHandler.GRID_SIZE;
						endOffset = (fingerTableEntry.getEnd() + RoutingHandler.GRID_SIZE - this.serverId) % RoutingHandler.GRID_SIZE;
						if (startOffset <= destinationOffset && destinationOffset <= endOffset) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 4,
										"Next hop for the destination " + destination
										+ " in " + fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd()
										+ " is " + fingerTableEntry.getSuccessor().serverId);
								return fingerTableEntry.getSuccessor();
						}
				}
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 5,
						"Next hop for the destination " + destination + " is this server");
				return this.server;
		}

//		@Override
//		public void setOptimalSuccessor(int startOfRange, Node successor) {
//				for (FingerTableEntry fingerTableEntry : fingerTable) {
//						if (fingerTableEntry.getStart() == startOfRange
//								&& fingerTableEntry.getSuccessor().serverId != successor.serverId) {
//								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 6,
//										"FT entry " + fingerTableEntry.getSuccessor().serverId
//										+ " in " + fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd()
//										+ " is replaced with " + successor.serverId);
//								fingerTableEntry.setSuccessor(successor.deepCopy());
//								if (this.server.getSinchanaTestInterface() != null) {
//										this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
//								}
//						}
//				}
//		}

		@Override
		public Node getOptimalSuccessor(int rServerId, int startOfRange) {
				int predecessorIdOffset = this.predecessor != null
						? (this.predecessor.serverId + RoutingHandler.GRID_SIZE - rServerId) % RoutingHandler.GRID_SIZE
						: 0;
				int startOffset = (startOfRange + RoutingHandler.GRID_SIZE - rServerId) % RoutingHandler.GRID_SIZE;
				int thisServerIdOffset = (this.server.serverId + RoutingHandler.GRID_SIZE - rServerId) % RoutingHandler.GRID_SIZE;
				if (startOffset <= predecessorIdOffset
						&& predecessorIdOffset < thisServerIdOffset) {
						return this.predecessor;
				} else {
						return this.server;
				}
		}

		@Override
		public Set<Node> getNeighbourSet() {
				Set<Node> neighbourSet = new HashSet<Node>();
				for (FingerTableEntry fingerTableEntry : fingerTable) {
						if (fingerTableEntry.getSuccessor() != null) {
								neighbourSet.add(fingerTableEntry.getSuccessor());
						}
				}
				if (this.predecessor != null) {
						neighbourSet.add(this.predecessor);
				}
				if (this.successor != null) {
						neighbourSet.add(this.successor);
				}
				return neighbourSet;
		}

		@Override
		public void setNeighbourSet(Set<Node> neighbourSet) {
				Iterator<Node> it = neighbourSet.iterator();
				for (; it.hasNext();) {
						updateTable(it.next());
				}
				optimizeFingerTable();
				if (this.server.getSinchanaTestInterface() != null) {
						this.server.getSinchanaTestInterface().setPredecessor(predecessor);
						this.server.getSinchanaTestInterface().setSuccessor(successor);
						this.server.getSinchanaTestInterface().setStable(true);
				}
		}

		@Override
		public void updateTable(Node node) {
				int newNodeOffset = (node.serverId + GRID_SIZE - this.serverId) % GRID_SIZE;
				int successorOffset = this.successor != null ? (this.successor.serverId + GRID_SIZE - this.serverId) % GRID_SIZE : 0;
				int predecessorOffset = this.predecessor != null ? (this.predecessor.serverId + GRID_SIZE - this.serverId) % GRID_SIZE : 0;
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 7,
						"Updating table: S:" + successorOffset
						+ " P:" + predecessorOffset
						+ " N:" + node.serverId + "->" + newNodeOffset);
				if (successorOffset == 0 || (newNodeOffset != 0 && newNodeOffset < successorOffset)) {
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 8,
								"Node " + node + " is set as successor overriding " + this.successor);
						this.successor = node.deepCopy();
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setSuccessor(successor);
						}
				}

				if (predecessorOffset == 0 || (newNodeOffset != 0 && predecessorOffset < newNodeOffset)) {
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 9,
								"Node " + node + " is set as predecessor overriding " + this.predecessor);
						this.predecessor = node.deepCopy();
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setPredecessor(predecessor);
						}
				}
				updateTableWithNode(node);
				if (!neighboursImported && this.serverId != this.predecessor.serverId) {
						importNeighbours(this.predecessor);
						neighboursImported = true;
				}
		}

		@Override
		public boolean isStable() {
				return this.stable;
		}

		@Override
		public void setStable(boolean isStable) {
				this.stable = isStable;
		}
}
