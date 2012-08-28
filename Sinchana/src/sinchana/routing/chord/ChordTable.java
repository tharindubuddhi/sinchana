/************************************************************************************

 * Sinchana Distributed Hash table 

 * Copyright (C) 2012 Sinchana DHT - Department of Computer Science &               
 * Engineering, University of Moratuwa, Sri Lanka. Permission is hereby 
 * granted, free of charge, to any person obtaining a copy of this 
 * software and associated documentation files of Sinchana DHT, to deal 
 * in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.

 * Neither the name of University of Moratuwa, Department of Computer Science 
 * & Engineering nor the names of its contributors may be used to endorse or 
 * promote products derived from this software without specific prior written 
 * permission.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
 * SOFTWARE.                                                                    
 ************************************************************************************/
package sinchana.routing.chord;

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
import sinchana.SinchanaDHT;
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class ChordTable implements RoutingHandler {

	private static final int SUCCESSOR_LEVELS = 3;
	private static final int NUMBER_OF_TABLE_ENTRIES = 3;
	private static final int TABLE_SIZE = 160;
	public static final BigInteger ZERO = new BigInteger("0", 16);
	public static final BigInteger ONE = new BigInteger("1", 16);
	public static final BigInteger TWO = new BigInteger("2", 16);
	private final FingerTableEntry[] fingerTable = new FingerTableEntry[TABLE_SIZE];
	private final SinchanaServer server;
	private final Node[] successors = new Node[SUCCESSOR_LEVELS];
	private final Node[] predecessors = new Node[SUCCESSOR_LEVELS];
	private final byte[] serverId;
	private final Node thisNode;
	private final BigInteger serverIdAsBigInt;
	private final Timer timer = new Timer();
	private int timeOutCount = 0;

	public ChordTable(SinchanaServer server) {
		this.server = server;
		this.thisNode = server.getNode();
		this.serverId = thisNode.serverId.array();
		this.serverIdAsBigInt = new BigInteger(1, serverId);
		this.timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (++timeOutCount >= SinchanaDHT.ROUTING_OPTIMIZATION_TIME_OUT) {
					timeOutCount = 0;
					optimize();
				}
			}
		}, 1000, 1000);
	}

	@Override
	public void init() {
		synchronized (fingerTable) {
			for (int i = 0; i < fingerTable.length; i++) {
				/**
				 * calculates start and end of the finger table entries in 2's power.
				 * initializes the table by setting this server as the successors for
				 * all the table entries.
				 */
				fingerTable[i] = new FingerTableEntry(NUMBER_OF_TABLE_ENTRIES);
				fingerTable[i].setStart(TWO.pow(i).add(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE));
				fingerTable[i].setStartOffset(SinchanaServer.GRID_SIZE.add(fingerTable[i].getStart()).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE));
				fingerTable[i].setEnd(TWO.pow(i + 1).add(serverIdAsBigInt).subtract(ONE).mod(SinchanaServer.GRID_SIZE));
				fingerTable[i].setEndOffset(SinchanaServer.GRID_SIZE.add(fingerTable[i].getEnd()).subtract(serverIdAsBigInt).mod(SinchanaServer.GRID_SIZE));
			}
		}
		this.initFingerTable();
	}

	private void initFingerTable() {
		synchronized (fingerTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				for (int j = 0; j < NUMBER_OF_TABLE_ENTRIES; j++) {
					fingerTable[i].setSuccessor((j == 0 ? thisNode.deepCopy() : null), j);
				}
			}
		}
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				predecessors[i] = null;
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				successors[i] = null;
			}
		}
	}

	@Override
	public void triggerOptimize() {
		timeOutCount = SinchanaDHT.ROUTING_OPTIMIZATION_TIME_OUT;
	}

	private void optimize() {
		Message msg = new Message(MessageType.DISCOVER_NEIGHBORS, thisNode, 2);
		msg.setFailedNodeSet(server.getConnectionPool().getFailedNodes());
		msg.setNeighbourSet(getNeighbourSet());
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

		int row = getRow(destination);
		if (row == -1) {
			return thisNode;
		} else {
			return fingerTable[row].getSuccessors()[0];
		}
	}

	@Override
	public boolean isInTheTable(Node nodeToCkeck) {
		byte[] id = nodeToCkeck.serverId.array();
		int row = getRow(id);
		Node[] entrySuccessors = fingerTable[row].getSuccessors();
		for (Node node : entrySuccessors) {
			if (node == null) {
				break;
			}
			if (Arrays.equals(node.serverId.array(), id)) {
				return true;
			}
		}
		for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
			if (predecessors[i] == null) {
				break;
			}
			if (Arrays.equals(predecessors[i].serverId.array(), id)) {
				return true;
			}
		}
		for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
			if (successors[i] == null) {
				break;
			}
			if (Arrays.equals(successors[i].serverId.array(), id)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Set<Node> getNeighbourSet() {
		//initializes an empty thisNode set.
		Set<Node> neighbourSet = new HashSet<Node>();
		synchronized (fingerTable) {
			for (FingerTableEntry fingerTableEntry : fingerTable) {
				/**
				 * add each successors in the finger table to the set if it is
				 * not this server it self. This is because the requester knows
				 * about this server, so sending it again is a waste.
				 */
				Node[] entrySuccessors = fingerTableEntry.getSuccessors();
				for (Node node : entrySuccessors) {
					if (node != null && !Arrays.equals(node.serverId.array(), this.serverId)) {
						neighbourSet.add(node);
					}
				}
			}
		}
		//adds the predecessors & the successors.
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (predecessors[i] != null) {
					neighbourSet.add(this.predecessors[i]);
				}
			}
		}
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (successors[i] != null) {
					neighbourSet.add(this.successors[i]);
				}
			}
		}
		//returns the thisNode set.
		return neighbourSet;
	}

	@Override
	public boolean updateTable(Node node, boolean add) {
		if (Arrays.equals(node.serverId.array(), this.serverId)) {
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
		BigInteger successorOffset, pointerOffset, tempNodeOffset, newNodeOffset = getOffset(node.serverId.array());
		Node tempNodeToUpdate;
		synchronized (successors) {
			pointerOffset = ZERO;
			tempNodeOffset = newNodeOffset;
			tempNodeToUpdate = node;
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				/**
				 * Checks whether the new thisNode should be set as the successors or not. 
				 * if successors is the server itself (successorOffset == 0) or if the new thisNode 
				 * is not the server it self (newNodeOffset != 0) and it successes 
				 * the server than the existing successors (newNodeOffset < successorOffset), 
				 * the new thisNode will be set as the successors.
				 * 0-----this.server.id------new.server.id-------existing.successors.id----------End.of.Grid
				 */
				successorOffset = successors[i] == null ? ZERO : getOffset(successors[i].serverId.array());
				if (pointerOffset.compareTo(newNodeOffset) == -1
						&& (successorOffset.equals(ZERO) || newNodeOffset.compareTo(successorOffset) == -1)) {
					Node temp = this.successors[i];
					this.successors[i] = tempNodeToUpdate.deepCopy();
					tempNodeToUpdate = temp;
					updatedSuccessors = true;
					pointerOffset = tempNodeOffset;
					tempNodeOffset = successorOffset;
				} else {
					pointerOffset = successorOffset;
				}
				if (tempNodeToUpdate == null || successors[i] == null) {
					break;
				}
			}
		}
		synchronized (predecessors) {
			pointerOffset = SinchanaServer.GRID_SIZE;
			tempNodeOffset = newNodeOffset;
			tempNodeToUpdate = node;
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				/**
				 * Checks whether the new thisNode should be set as the predecessors or not. 
				 * if predecessors is the server itself (predecessorOffset == 0) or if the new thisNode 
				 * is not the server it self (newNodeOffset != 0) and it predecesses 
				 * the server than the existing predecessors (predecessorOffset < newNodeOffset), 
				 * the new thisNode will be set as the predecessors.
				 * 0-----existing.predecessors.id------new.server.id-------this.server.id----------End.of.Grid
				 */
				successorOffset = predecessors[i] == null ? ZERO : getOffset(predecessors[i].serverId.array());
				if (newNodeOffset.compareTo(pointerOffset) == -1
						&& (successorOffset.equals(ZERO) || successorOffset.compareTo(newNodeOffset) == -1)) {
					Node temp = this.predecessors[i];
					this.predecessors[i] = tempNodeToUpdate.deepCopy();
					tempNodeToUpdate = temp;
					updatedPredecessors = true;
					pointerOffset = tempNodeOffset;
					tempNodeOffset = successorOffset;
				} else {
					pointerOffset = successorOffset;
				}
				if (tempNodeToUpdate == null || predecessors[i] == null) {
					break;
				}
			}
		}
		synchronized (fingerTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				if (fingerTable[i].getStartOffset().compareTo(newNodeOffset) != 1) {
					tempNodeToUpdate = node;
					tempNodeOffset = newNodeOffset;
					pointerOffset = ZERO;
					for (int j = 0; j < NUMBER_OF_TABLE_ENTRIES; j++) {
						/**
						 * Checks whether the new thisNode should be set as the successors or not. 
						 * if the new thisNode successes the table entry (startOffset <= newNodeOffset) and
						 * if it is a much suitable successors than the existing one 
						 * (newNodeOffset < successorOffset || successorOffset == 0), 
						 * the new thisNode will be set as the successors for that fingertable entry.
						 * 0-----finger.table.entry.start------new.server.id-------existing.successors.id----------End.of.Grid
						 */
						Node entry = fingerTable[i].getSuccessors()[j];
						successorOffset = entry == null ? ZERO : getOffset(entry.serverId.array());
						if (pointerOffset.compareTo(tempNodeOffset) == -1
								&& (successorOffset.equals(ZERO) || tempNodeOffset.compareTo(successorOffset) == -1)) {
							fingerTable[i].setSuccessor(tempNodeToUpdate.deepCopy(), j);
							tempNodeToUpdate = entry;
							updatedTable = true;
							pointerOffset = tempNodeOffset;
							tempNodeOffset = successorOffset;
						} else {
							pointerOffset = successorOffset;
						}
						if (tempNodeToUpdate == null || fingerTable[i].getSuccessors()[j] == null) {
							break;
						}
					}
				} else {
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

	private int getRow(byte[] id) {
		BigInteger offset = getOffset(id);
		byte[] val = offset.toByteArray();
		int len = val.length;
		for (int i = len - 20; i < len; i++) {
			if (i >= 0) {
				for (int j = 7; j >= 0; j--) {
					if ((val[i] & (1 << j)) != 0) {
						return (TABLE_SIZE - ((i - len + 20) * 8 + (8 - j)));
					}
				}
			}
		}
		return -1;
	}

	/**
	 * Returns the offset of the id relative to the this server.
	 * <code>(id + RoutingHandler.GRID_SIZE - this.server.serverId) % RoutingHandler.GRID_SIZE;</code>
	 * @param id	Id to calculate the offset.
	 * @return		Offset of the id relative to this server.
	 */
	private BigInteger getOffset(byte[] id) {
		for (int i = 0; i < 20; i++) {
			if ((this.serverId[i] + 256) % 256 > (id[i] + 256) % 256) {
				return SinchanaServer.GRID_SIZE.add(new BigInteger(1, id)).subtract(serverIdAsBigInt);
			} else if ((this.serverId[i] + 256) % 256 < (id[i] + 256) % 256) {
				return new BigInteger(1, id).subtract(serverIdAsBigInt);
			}
		}
		return ZERO;
	}

	@Override
	public void printInfo() {
		final String spaces = "****************************************";
		System.out.println("--------------" + this.server.getServerIdAsString() + "--------------");
		System.out.println("Routing Table");
		synchronized (fingerTable) {
			for (int i = 0; i < TABLE_SIZE; i++) {
				System.out.print(ByteArrays.idToReadableString(fingerTable[i].getStart().toByteArray())
						+ "\t" + ByteArrays.idToReadableString(fingerTable[i].getEnd().toByteArray())
						+ "\t\t");
				for (int j = 0; j < NUMBER_OF_TABLE_ENTRIES; j++) {
					if (fingerTable[i].getSuccessors()[j] != null) {
						System.out.print(" " + ByteArrays.idToReadableString(fingerTable[i].getSuccessors()[j]));
					} else {
						System.out.print(" " + spaces);
					}
				}
				System.out.println("");
			}
		}
		System.out.println("Predecessors");
		synchronized (predecessors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (predecessors[i] != null) {
					System.out.print(" " + ByteArrays.idToReadableString(predecessors[i].serverId));
				} else {
					System.out.print(" " + spaces);
				}
			}
		}
		System.out.println("\nSuccessors");
		synchronized (successors) {
			for (int i = 0; i < SUCCESSOR_LEVELS; i++) {
				if (successors[i] != null) {
					System.out.print(" " + ByteArrays.idToReadableString(successors[i].serverId));
				} else {
					System.out.print(" " + spaces);
				}
			}
		}
		System.out.println("\n");
	}
}
