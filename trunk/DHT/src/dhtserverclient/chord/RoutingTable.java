/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dhtserverclient.chord;

import dhtserverclient.PortHandler;
import dhtserverclient.RoutingHandler;
import dhtserverclient.Server;
import dhtserverclient.thrift.Message;
import dhtserverclient.thrift.MessageType;
import dhtserverclient.thrift.Node;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

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
		private boolean neighboursImported = false;
		private int stabilityCount = 0;

		public RoutingTable(Server server) {
				Logger.getLogger(RoutingTable.class.getName()).setLevel(Level.WARNING);
				this.server = server;
				this.serverId = server.getServerId();
				initFingerTable();
				updateTableWithNode(server);
				neighboursImported = true;
		}

		public RoutingTable(Server server, Node anotherNode) {
				Logger.getLogger(RoutingTable.class.getName()).setLevel(Level.WARNING);
				this.server = server;
				this.serverId = server.getServerId();
				initFingerTable();
				updateTableWithNode(server);
				updateTableWithNode(anotherNode);
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

		private void updateTableWithNode(Node node) {
				int start, successorId, tempNodeId;
				boolean tableChanged = false;
				tempNodeId = (node.serverId + GRID_SIZE - this.serverId) % GRID_SIZE;
				for (int i = 0; i < fingerTable.length; i++) {
						FingerTableEntry fingerTableEntry = fingerTable[i];
						if (fingerTableEntry.getSuccessor() != null) {
								start = (fingerTableEntry.getStart() + GRID_SIZE - this.serverId) % GRID_SIZE;
								successorId = (fingerTableEntry.getSuccessor().serverId + GRID_SIZE - this.serverId) % GRID_SIZE;
								if (start <= tempNodeId && (tempNodeId < successorId || successorId == 0)) {
//										System.out.println(this.serverId + ": Setting "
//												+ node.serverId + " as successor of "
//												+ fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd()
//												+ " overriding " + fingerTableEntry.getSuccessor().serverId);
//										System.out.println(this.serverId 
//												+ ": calculated s:" + start +" e:" +end 
//												+ " sc:" + successorId + " n:" + tempNodeId);
										fingerTableEntry.setSuccessor(node.deepCopy());
										tableChanged = true;
								}
						} else {
//								System.out.println(this.serverId + ": Setting " 
//												+ node.serverId +" as successor of " 
//												+ fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd() 
//												+ " since no other node present");										
								fingerTableEntry.setSuccessor(node.deepCopy());
								tableChanged = true;
						}
				}
				if (tableChanged && this.server.getSinchanaTestInterface() != null) {
						this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
				}
		}

		private void importNeighbours(Node neighbour) {
				if (!neighboursImported) {
//						System.out.println(this.serverId + ": Importing neighbour set from " + neighbour.serverId);
						Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBOURS, 256);
						this.server.getPortHandler().send(msg, neighbour.address, neighbour.portId);
				}
				neighboursImported = true;
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
//								System.out.println(this.serverId
//										+ ": Next hop for the destination " + destination
//										+ " in " + fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd()
//										+ " is " + fingerTableEntry.getSuccessor().serverId);
								return fingerTableEntry.getSuccessor();
						}
				}
//				System.out.println(this.serverId + ": Next hop for the destination " + destination + " is this server");
				return this.server;
		}

		@Override
		public void setOptimalSuccessor(int startOfRange, Node successor) {
				for (FingerTableEntry fingerTableEntry : fingerTable) {
						if (fingerTableEntry.getStart() == startOfRange
								&& fingerTableEntry.getSuccessor().serverId != successor.serverId) {
//								System.out.println(this.serverId
//										+ ": FT entry " + fingerTableEntry.getSuccessor().serverId
//										+ " in " + fingerTableEntry.getStart() + "-" + fingerTableEntry.getEnd()
//										+ " is replaced with " + successor.serverId);
								fingerTableEntry.setSuccessor(successor.deepCopy());
//								this.server.getUi().setTableInfo(fingerTable);
						}
				}
		}

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
				for (Iterator<Node> it = neighbourSet.iterator(); it.hasNext();) {
						updateTableWithNode(it.next());
				}
				optimizeFingerTable();
				if(this.server.getSinchanaTestInterface() != null){
						this.server.getSinchanaTestInterface().setPredecessor(predecessor);
						this.server.getSinchanaTestInterface().setSuccessor(successor);
						this.server.getSinchanaTestInterface().setStable(true);
				}
		}

		@Override
		public void updateTable(Node node) {
				int newNodeOffset = (node.serverId + GRID_SIZE - this.serverId) % GRID_SIZE;
				int successorOffset = this.successor != null ? (this.successor.serverId + GRID_SIZE - this.serverId) % GRID_SIZE : -1;
				int predecessorOffset = this.predecessor != null ? (this.predecessor.serverId + GRID_SIZE - this.serverId) % GRID_SIZE : -1;
//				System.out.println(this.serverId + ": Updating table: "
//						+ " S " + "->" + successorOffset
//						+ " P " + "->" + predecessorOffset
//						+ " N " + node.serverId + "->" + newNodeOffset);

				if (this.successor == null || newNodeOffset < successorOffset) {
//						Logger.getLogger(RoutingTable.class.getName()).logp(Level.INFO,
//								RoutingTable.class.getName(), "Server " + this.serverId,
//								"Node " + node + " is set as successor overriding " + this.successor);
						this.successor = node.deepCopy();
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setSuccessor(successor);
						}
				}

				if (this.predecessor == null || predecessorOffset < newNodeOffset) {
//						Logger.getLogger(RoutingTable.class.getName()).logp(Level.INFO,
//								RoutingTable.class.getName(), "Server " + this.serverId,
//								"Node " + node + " is set as predecessor overriding " + this.predecessor);
						this.predecessor = node.deepCopy();
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setPredecessor(predecessor);
						}
				}

				updateTableWithNode(node);
		}

		@Override
		public synchronized void incStabilityCount() {
				this.stabilityCount++;
				if (this.stabilityCount == 2) {
						importNeighbours(this.predecessor);
				}
		}

		@Override
		public boolean isStable() {
				return (this.stabilityCount >= 2 || this.serverId == 0);
		}
}
