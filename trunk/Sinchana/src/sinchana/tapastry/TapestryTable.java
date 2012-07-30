/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.tapastry;

import java.util.logging.Level;
import sinchana.PortHandler;
import sinchana.RoutingHandler;
import sinchana.Server;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Hiru
 */
public class TapestryTable implements RoutingHandler, Runnable {

		public static final int TABLE_SIZE = (int) Math.log10(Server.GRID_SIZE);

		static {
				System.out.println("TAPESTRY: \tGrid size: " + Server.GRID_SIZE + " vs Table size: " + TABLE_SIZE);
				if (Server.GRID_SIZE - ((int) Math.pow(10, TABLE_SIZE)) != 0) {
						throw new RuntimeException("GRID SIZE defined in Server.class is invalid");
				}
		}
		private Server server;
		private Node successor = null;
		private Node predecessor = null;
		private long serverId;
		private Node[][] fingerTable = new Node[TABLE_SIZE][10];
		private boolean neighboursImported = false;

		/**
		 * Class constructor with the server instance where the routing table is initialize.
		 * @param server		Server instance. The routing table will be initialize based on this.
		 */
		public TapestryTable(Server server) {
				this.server = server;
		}

		/**
		 * Initialize routing table. This resets the routing table and calls 
		 * <code>initFingerTable</code> 
		 */
		@Override
		public void init() {
				this.serverId = this.server.getServerId();
				synchronized (this) {
						this.initFingerTable();
						this.neighboursImported = false;
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
				this.predecessor = this.server.deepCopy();
				this.successor = this.server.deepCopy();
				if (this.server.getSinchanaTestInterface() != null) {
						this.server.getSinchanaTestInterface().setPredecessor(predecessor);
						this.server.getSinchanaTestInterface().setSuccessor(successor);
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
								if (node != null && node.serverId != this.serverId) {
										msg = new Message(this.server, MessageType.FIND_SUCCESSOR, Server.MESSAGE_LIFETIME);
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
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 3,
						"Importing neighbour set from " + neighbour.serverId);
				Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBOURS, 256);
				this.server.getPortHandler().send(msg, neighbour);
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
		public Node getNextNode(long destination) {

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
								if (node != null && node.serverId != this.serverId) {
										neighbourSet.add(node);
								}
						}

				}
				//adds the predecessor.
				if (this.predecessor != null && this.predecessor.serverId != this.serverId) {
						neighbourSet.add(this.predecessor);
				}
				//adds the successor.
				if (this.successor != null && this.successor.serverId != this.serverId) {
						neighbourSet.add(this.successor);
				}
				//returns the node set.
				return neighbourSet;
		}

		@Override
		public void getOptimalSuccessor(Message message) {
		}

		@Override
		public void updateTable(Node node) {
				if (node.serverId == this.serverId) {
						return;
				}
				//calculates offsets for the ids.
				long newNodeOffset = getOffset(node.serverId);
				boolean tableChanged = false;
				synchronized (this) {
						long successorOffset = getOffset(this.successor.serverId);
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
						if (successorOffset == 0 || (newNodeOffset != 0 && newNodeOffset < successorOffset)) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 8,
										"Node " + node + " is set as successor overriding " + this.successor);
								this.successor = node.deepCopy();
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
						long predecessorOffset = getOffset(this.predecessor.serverId);
						if (predecessorOffset == 0 || (newNodeOffset != 0 && predecessorOffset < newNodeOffset)) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_ROUTING_TABLE, 9,
										"Node " + node + " is set as predecessor overriding " + this.predecessor);
								this.predecessor = node.deepCopy();
								if (this.server.getSinchanaTestInterface() != null) {
										this.server.getSinchanaTestInterface().setPredecessor(predecessor);
								}
						}

						int raw = getRaw(this.serverId, node.serverId);
						int column = getColumn(node.serverId, raw);
						fingerTable[raw][column] = node.deepCopy();
//						System.out.println(this.serverId + ": [" + raw + "," + column + "] = " + node.serverId);
				}
				/**
				 * updates the testing interfaces if the table is altered.
				 */
				if (tableChanged && this.server.getSinchanaTestInterface() != null) {
//						this.server.getSinchanaTestInterface().setRoutingTable(fingerTable);
				}

				/**
				 * if the neighbor set has not imported, imports it from the predecessor.
				 * This will happens only once after the node successfully joined to the grid.
				 */
				if (!neighboursImported && this.serverId != this.predecessor.serverId) {
						importNeighbours(this.predecessor);
						neighboursImported = true;
				}
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
		 * Remove the node from the routing table, temporary update the table and 
		 * send messages to the nodes in the routing table to get the optimal neighbors.
		 * 
		 * @param nodeToRemove Node to remove from the finger table. 
		 */
		@Override
		public void removeNode(Node nodeToRemove) {
				//gets the known node set.
				Set<Node> neighbourSet = getNeighbourSet();
				//reset the predecessor, successor and finger table entries.
				synchronized (this) {
						initFingerTable();
						for (Node node : neighbourSet) {
								/**
								 * updates predecessor, successor and finger table entries
								 * back, with the known node set, if the node is not equal 
								 * to the node which is to be remove.
								 */
								if (node.serverId != nodeToRemove.serverId) {
										updateTable(node);
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
		private long getOffset(long id) {
				return (id + Server.GRID_SIZE - this.server.serverId) % Server.GRID_SIZE;
		}

		private int getRaw(long id, long newId) {
				int factor; 
				long t1, t2;
				for (int i = TABLE_SIZE - 1; i >= 0; i--) {
						factor = (int) Math.pow(10, i);
						t1 = id / factor;
						t2 = newId / factor;
						if (t1 != t2) {
								return i;
						}
				}
				return -1;
		}

		private int getColumn(long id, int raw) {
				return (int)(id % ((long) Math.pow(10, raw + 1)) / ((long) Math.pow(10, raw)));
		}
}
