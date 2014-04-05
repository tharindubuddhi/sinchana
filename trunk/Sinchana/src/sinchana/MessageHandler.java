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
package sinchana;

import sinchana.service.SinchanaServiceInterface;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import sinchana.util.tools.ByteArrays;

/**
 *This class is responsible for all the message handling stuffs. All the incoming 
 * messages are queued in the incomingMessageQueue and one thread takes messages 
 * one by one and process them according to their type.
 * @author Hiru
 */
public class MessageHandler {

	/*A collection used to keep a set of nodes temporaly*/
	private final Set<Node> nodes = new HashSet<Node>();
	/*Sinchana server instance*/
	private final SinchanaServer server;
	/*Node info*/
	private final Node thisNode;
	/*Server id*/
	private final byte[] serverId;
	/*The server id as a big integer value*/
	private final BigInteger serverIdAsBigInt;
	/*A semaphore which is used to control incoming request flow*/
	private final Semaphore messageQueueLock = new Semaphore(0);
	/*A support variable for the messageQueueLock*/
	private boolean waitOnMessageQueueLock = false;
	/**
	 * Message queue to buffer incoming messages. The size of the queue is 
	 * determined by MESSAGE_BUFFER_SIZE.
	 */
	private final ArrayBlockingQueue<Message> incomingMessageQueue = new ArrayBlockingQueue<Message>(SinchanaDHT.INPUT_MESSAGE_BUFFER_SIZE);
	private final Thread incomingMessageQueueThread = new Thread(new Runnable() {

		private boolean joined = false;

		/**
		 * A node's join is considered completed if and only if 
		 * it's predecessor and successor are set. Typically, 
		 * receiving at least 1 of 2 MessageType.JOIN messages it 
		 * sends at the beginning is enough to identify and to set 
		 * predecessor and successor. Until the node receives 1 of 
		 * those 2 messages, all the other messages except DISCOVER_NEIGHBORS
		 * and JOIN are added back to the queue to process later.
		 * Once the node receives it's JOIN message, the joining is 
		 * completed and all the messages are processed with no restriction.
		 */
		@Override
		public void run() {
			while (true) {

				/*Update test interface*/
				if (server.getSinchanaTestInterface() != null) {
					server.getSinchanaTestInterface().setMessageQueueSize(incomingMessageQueue.size());
				}
				try {

					/*Take the first message from the queue. If the queue is empty, wait*/
					Message message = incomingMessageQueue.take();
					if (joined) {

						/*If the join process has completed, process messages normally*/
						processMessage(message);
						if (waitOnMessageQueueLock) {

							/*Release if any thread is waiting on the semaphore*/
							messageQueueLock.release();
						}
					} else {
						/*If the joining process has not completed, follow special procedure*/
						switch (message.type) {
							case JOIN:

								/*If the message type is JOIN and it's originated by this node*/
								if (Arrays.equals(message.source.serverId.array(), serverId)) {

									/*join status is checked and updated*/
									joined = message.isSuccess();
									server.setJoined(joined, message.getError());
									if (server.getSinchanaTestInterface() != null) {
										server.getSinchanaTestInterface().setStable(joined);
									}
								}
							case DISCOVER_NEIGHBORS:

								/*DISCOVER_NEIGHBORS messages are processed normally, 
								 * since they are not supposed to route again*/
								processMessage(message);
								if (waitOnMessageQueueLock) {
									messageQueueLock.release();
								}
								break;
							case TEST_RING:
							case REQUEST:
							case STORE_DATA:
							case DELETE_DATA:
							case GET_DATA:
							case GET_SERVICE:
							case ERROR:
							case RESPONSE:
							case RESPONSE_DATA:
							case RESPONSE_SERVICE:
							case ACKNOWLEDGE_DATA_STORE:
							case ACKNOWLEDGE_DATA_REMOVE:
							default:

								/*All the other messages which need routing are 
								 * added back to the queue until the join is completed*/
								incomingMessageQueue.put(message);
						}
					}
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
			}
		}
	});

	/**
	 * Constructor of the class. The server instance is passed as an argument.
	 * @param server SinchanaServer instance. 
	 */
	MessageHandler(SinchanaServer server) {
		this.server = server;
		this.thisNode = server.getNode();
		this.serverId = thisNode.getServerId();
		this.serverIdAsBigInt = new BigInteger(1, this.serverId);
		incomingMessageQueueThread.start();
	}

	/*
	 * Adds a message to the incoming message queue.
	 */
	boolean queueMessage(Message message) {

		/*Updates the test interface*/
		if (server.getSinchanaTestInterface() != null) {
			server.getSinchanaTestInterface().incIncomingMessageCount();
			server.getSinchanaTestInterface().setMessageQueueSize(incomingMessageQueue.size());
		}
		synchronized (incomingMessageQueue) {
			return incomingMessageQueue.offer(message);
		}
	}

	/*
	 * Adds a request to the incoming message queue. If the queue is not empty,
	 * the thread is kept waiting.
	 */
	void addRequest(Message message) throws InterruptedException {

		/*Updates the test interface*/
		if (server.getSinchanaTestInterface() != null) {
			server.getSinchanaTestInterface().incIncomingMessageCount();
			server.getSinchanaTestInterface().setMessageQueueSize(incomingMessageQueue.size());
		}
		boolean success = false;
		synchronized (incomingMessageQueue) {
			success = incomingMessageQueue.size() == 0 && incomingMessageQueue.offer(message);
		}
		while (!success) {
			waitOnMessageQueueLock = true;
			messageQueueLock.acquire();
			synchronized (incomingMessageQueue) {
				success = incomingMessageQueue.size() == 0 && incomingMessageQueue.offer(message);
			}
		}
	}

	/*
	 * Process a message from the incoming message queue. 
	 */
	private synchronized void processMessage(Message message) {

		/*Update the routing table with the message*/
		this.updateTableWithMessage(message);

		/*Categorize the message according to the message size and direct them to the relavent methods*/
		switch (message.type) {

			/*These messages are required rouint, so they are directed to the routing method*/
			case REQUEST:
			case STORE_DATA:
			case DELETE_DATA:
			case GET_DATA:
			case GET_SERVICE:
				this.processRouting(message);
				break;

			case JOIN:
				this.processJoin(message);
				break;
			case DISCOVER_NEIGHBORS:
				this.processDiscoverNeighbours(message);
				break;
			case TEST_RING:
				this.processTestRing(message);
				break;

			case ERROR:
			case RESPONSE:
			case RESPONSE_DATA:
			case RESPONSE_SERVICE:
			case ACKNOWLEDGE_DATA_STORE:
			case ACKNOWLEDGE_DATA_REMOVE:
				
				/*these messages are the responses which are destinated to this node, so the are directed to set response*/
				this.server.getClientHandler().setResponse(message);
				break;
		}
	}

	/*
	 * Updates the table with the message content.
	 */
	private void updateTableWithMessage(Message message) {
		
		/*Empty the temporary node set*/
		nodes.clear();
		boolean updated = false;
		
		/*If failed node infomations are available, they are removed from the routing table*/
		if (message.isSetFailedNodeSet()) {
			Set<Node> failedNodeSet = message.getFailedNodeSet();
			for (Node node : failedNodeSet) {
				if (Arrays.equals(node.serverId.array(), this.serverId)) {
					continue;
				}
				this.server.getConnectionPool().updateNodeInfo(node, false);
				if (this.server.getRoutingHandler().isInTheTable(node)) {
					updated = this.server.getRoutingHandler().updateTable(node, false) || updated;
				}
			}
		}
		
		/*Adds source and the station (last hop) to the set*/
		nodes.add(message.source);
		nodes.add(message.station);
		
		/*If the neighbor set information are available, add them too to the node set*/
		if (message.isSetNeighbourSet()) {
			nodes.addAll(message.getNeighbourSet());
		}
		
		/*Updates the table with the nodes in the tempory node set*/
		if (!nodes.isEmpty()) {
			long time = System.currentTimeMillis();
			for (Node node : nodes) {
				if (Arrays.equals(node.serverId.array(), this.serverId)) {
					continue;
				}
				if (server.getConnectionPool().hasReportFailed(node)) {
					Connection failedConnection = server.getConnectionPool().getConnection(node);
					if ((failedConnection.getLastHeardFailedTime() + SinchanaDHT.FAILED_REACCEPT_TIME_OUT * 1000) < time) {
						failedConnection.reset();
					} else {
						continue;
					}
				}
				this.server.getConnectionPool().updateNodeInfo(node, true);
				if (!this.server.getRoutingHandler().isInTheTable(node)) {
					updated = this.server.getRoutingHandler().updateTable(node, true) || updated;
				}
			}
		}
		
		/*If the table has changed, triggers table optimize*/
		if (updated) {
			this.server.getRoutingHandler().triggerOptimize();
		}
	}

	/*
	 * This method routes the message to the destination according to their destination id
	 * and to the routing table.
	 */
	private void processRouting(Message message) {
		Node predecessor = this.server.getRoutingHandler().getPredecessors()[0];
		
		/*calculates offsets*/
		BigInteger targetKeyOffset = getOffset(message.getDestinationId());
		BigInteger predecessorOffset = (predecessor == null ? BigInteger.ZERO : getOffset(predecessor.serverId.array()));
		BigInteger prevStationOffset = getOffset(message.station.serverId.array());

		/*If the destination id lies in between the predecessor and this node it self, 
		 * the message is considered to be belong to this node, so they are delivered. 
		 * otherwise, they are routed.*/
		if (predecessorOffset.compareTo(targetKeyOffset) == -1 || targetKeyOffset.equals(BigInteger.ZERO)) {
			deliverMessage(message);
		} else {
			if (!prevStationOffset.equals(BigInteger.ZERO)
					&& prevStationOffset.compareTo(targetKeyOffset) == -1) {
				/*
				 * This should be an errornous receive. Sending to the predecessor.
				 */
				message.setRoutedViaPredecessors(true);
				this.server.getIOHandler().send(message, predecessor);

			} else {
				Node nextHop = this.server.getRoutingHandler().getNextNode(message.getDestinationId());
				this.server.getIOHandler().send(message, nextHop);
			}
		}
	}

	/*
	 * Processes node joining.
	 */
	private void processJoin(Message message) {
		if (!Arrays.equals(message.source.serverId.array(), serverId)) {
			if (this.server.getConnectionPool().hasReportFailed(message.source)) {
				Connection connection = this.server.getConnectionPool().getConnection(message.source);
				long remainingTime = Math.max(connection.getLastHeardFailedTime(), connection.getLastKnownFailedTime())
						+ SinchanaDHT.FAILED_REACCEPT_TIME_OUT * 1000 - System.currentTimeMillis();
				if (remainingTime > 0) {
					message.setSuccess(false);
					message.setError(SinchanaDHT.ERROR_MSG_JOIN_REACCEPTANCE);
					server.getIOHandler().send(message, message.source);
				}
				return;
			}
			message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
			message.setSuccess(true);

			BigInteger newServerIdOffset = getOffset(message.source.serverId.array());
			BigInteger prevStationIdOffset = getOffset(message.station.serverId.array());
			BigInteger tempNodeOffset, nextPredecessorOffset, nextSuccessorOffset;
			Node nextSuccessor = thisNode;
			Node nextPredecessor = thisNode;
			Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
			for (Node node : neighbourSet) {
				tempNodeOffset = getOffset(node.serverId.array());
				nextPredecessorOffset = getOffset(nextPredecessor.serverId.array());
				nextSuccessorOffset = getOffset(nextSuccessor.serverId.array());

				if (newServerIdOffset.compareTo(prevStationIdOffset) != 1
						&& (nextSuccessorOffset.compareTo(tempNodeOffset) == -1
						|| nextSuccessorOffset.equals(BigInteger.ZERO))
						&& tempNodeOffset.compareTo(newServerIdOffset) == -1) {
					nextSuccessor = node;
				}
				if (prevStationIdOffset.compareTo(newServerIdOffset) != 1
						&& (tempNodeOffset.compareTo(nextPredecessorOffset) == -1
						|| nextPredecessorOffset.equals(BigInteger.ZERO))
						&& newServerIdOffset.compareTo(tempNodeOffset) == -1) {
					nextPredecessor = node;
				}
			}
			if (!Arrays.equals(nextPredecessor.serverId.array(), serverId)) {
				server.getIOHandler().send(message.deepCopy(), nextPredecessor);
			} else {
				server.getIOHandler().send(message.deepCopy(), message.source);
			}
			if (!Arrays.equals(nextSuccessor.serverId.array(), serverId)) {
				server.getIOHandler().send(message.deepCopy(), nextSuccessor);
			} else {
				server.getIOHandler().send(message.deepCopy(), message.source);
			}
		}
	}

	/*
	 * Responses to the DISCOVER_NEIGHBORS messages.
	 */
	private void processDiscoverNeighbours(Message message) {
		if (!Arrays.equals(message.source.serverId.array(), serverId)) {
			message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
			message.setFailedNodeSet(this.server.getConnectionPool().getFailedNodes());
			this.server.getIOHandler().send(message, message.source);
		}
	}

	/*
	 * Routes TEST_RING messages.
	 */
	private void processTestRing(Message message) {
		Node predecessor = this.server.getRoutingHandler().getPredecessors()[0];
		Node successor = this.server.getRoutingHandler().getSuccessors()[0];
		if (predecessor == null) {
			predecessor = thisNode;
		}
		if (successor == null) {
			successor = thisNode;
		}
		if (Arrays.equals(message.source.serverId.array(), serverId)) {
			if (message.isSetData()) {
				System.out.println("Ring test completed - length: "
						+ (new String(message.getData()).split(SinchanaDHT.TEST_RING_SEPARATOR).length)
						+ " :: " + new String(message.getData()));
			} else {
				message.setData(this.server.getServerIdAsString().getBytes());
				this.server.getIOHandler().send(message.deepCopy(), predecessor);
				this.server.getIOHandler().send(message.deepCopy(), successor);
			}
		} else {
			if (Arrays.equals(message.station.serverId.array(), predecessor.serverId.array())) {
				message.setData((new String(message.getData()) + SinchanaDHT.TEST_RING_SEPARATOR + this.server.getServerIdAsString()).getBytes());
				this.server.getIOHandler().send(message, successor);
			} else if (Arrays.equals(message.station.serverId.array(), successor.serverId.array())) {
				message.setData((new String(message.getData()) + SinchanaDHT.TEST_RING_SEPARATOR + this.server.getServerIdAsString()).getBytes());
				this.server.getIOHandler().send(message, predecessor);
			} else {
				Logger.log(thisNode, Logger.LogLevel.LEVEL_WARNING, Logger.LogClass.CLASS_MESSAGE_HANDLER, 5,
						"Message Terminated! Received from " + ByteArrays.idToReadableString(message.station)
						+ " which is neither predecessor or successor.");
			}
		}
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
		return BigInteger.ZERO;
	}

	/**	 
	 * Method which executes when the message is delivered to the relevant recipient node
	 */
	private void deliverMessage(Message message) {
		Message returnMessage = null;
		boolean handlerAvailable;
		boolean responseExpected = message.isSetResponseExpected() && message.responseExpected;
		if (responseExpected) {
			returnMessage = new Message();
			returnMessage.setSource(thisNode);
			returnMessage.setLifetime(1);
			returnMessage.setDestination(message.source);
			returnMessage.setDestinationId(message.source.serverId);
			returnMessage.setId(message.getId());
			returnMessage.setKey(message.getKey());
		}
		switch (message.type) {
			case REQUEST:
				handlerAvailable = this.server.getSinchanaRequestCallback() != null;
				if (responseExpected) {
					returnMessage.setType(MessageType.RESPONSE);
					returnMessage.setSuccess(handlerAvailable);
					returnMessage.setLifetime(1 + message.lifetime);
					returnMessage.setRoutedViaPredecessors(message.routedViaPredecessors);
					if (handlerAvailable) {
						returnMessage.setData(this.server.getSinchanaRequestCallback().request(message.getData()));
					} else {
						returnMessage.setError(SinchanaDHT.ERROR_MSG_RESPONSE_HANDLER_NOT_FOUND);
					}
				} else if (handlerAvailable) {
					this.server.getSinchanaRequestCallback().request(message.getData());
				}
				break;
			case STORE_DATA:
				handlerAvailable = this.server.getSinchanaDataStoreInterface() != null;
				if (responseExpected) {
					returnMessage.setType(MessageType.ACKNOWLEDGE_DATA_STORE);
					returnMessage.setSuccess(handlerAvailable
							&& this.server.getSinchanaDataStoreInterface().store(message.getKey(), message.getData()));
					if (!handlerAvailable) {
						returnMessage.setError(SinchanaDHT.ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND);
					}
				} else if (handlerAvailable) {
					this.server.getSinchanaDataStoreInterface().store(message.getKey(), message.getData());
				}
				break;
			case DELETE_DATA:
				handlerAvailable = this.server.getSinchanaDataStoreInterface() != null;
				if (responseExpected) {
					returnMessage.setType(MessageType.ACKNOWLEDGE_DATA_REMOVE);
					returnMessage.setSuccess(handlerAvailable
							&& this.server.getSinchanaDataStoreInterface().remove(message.getKey()));
					if (!handlerAvailable) {
						returnMessage.setError(SinchanaDHT.ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND);
					}
				} else if (handlerAvailable) {
					this.server.getSinchanaDataStoreInterface().remove(message.getKey());
				}
				break;
			case GET_DATA:
				handlerAvailable = this.server.getSinchanaDataStoreInterface() != null;
				if (responseExpected) {
					returnMessage.setType(MessageType.RESPONSE_DATA);
					returnMessage.setSuccess(handlerAvailable);
					if (handlerAvailable) {
						returnMessage.setData(this.server.getSinchanaDataStoreInterface().get(message.getKey()));
					} else {
						returnMessage.setError(SinchanaDHT.ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND);
					}
				} else if (handlerAvailable) {
					this.server.getSinchanaDataStoreInterface().get(message.getKey());
				}
				break;
			case GET_SERVICE:
				SinchanaServiceInterface ssi = this.server.getSinchanaServiceStore().get(message.getKey());
				handlerAvailable = ssi != null;
				if (responseExpected) {
					returnMessage.setType(MessageType.RESPONSE_SERVICE);
					returnMessage.setSuccess(handlerAvailable);
					if (handlerAvailable) {
						returnMessage.setData(ssi.process(message.getKey(), message.getData()));
					} else {
						returnMessage.setError(SinchanaDHT.ERROR_MSG_SERVICE_HANDLER_NOT_FOUND);
					}
				} else if (handlerAvailable) {
					ssi.process(message.getKey(), message.getData());
				}
				break;
		}
		if (responseExpected) {
			if (Arrays.equals(serverId, returnMessage.destination.serverId.array())) {
				this.server.getClientHandler().setResponse(returnMessage);
			} else {
				this.server.getIOHandler().send(returnMessage, returnMessage.destination);
			}
		}
	}
}
