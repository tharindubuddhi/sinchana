/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.service.SinchanaServiceInterface;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import sinchana.connection.Connection;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class MessageHandler {

	private final SinchanaServer server;
	private final Node thisNode;
	private final byte[] serverId;
	private final BigInteger serverIdAsBigInt;
	private final BigInteger ZERO = new BigInteger("0", CONFIGURATIONS.NUMBER_BASE);
	private final Semaphore messageQueueLock = new Semaphore(0);
	private boolean waitOnMessageQueueLock = false;
	private static final String ERROR_MSG_JOIN_REACCEPTANCE = "You have to wait at least (ms) ";
	private static final String ERROR_MSG_RESPONSE_HANDLER_NOT_FOUND = "Response handler not found";
	private static final String ERROR_MSG_SERVICE_HANDLER_NOT_FOUND = "Service handler not found";
	private static final String ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND = "Data handler not found";
	private static final String TEST_RING_SEPARATOR = " > ";
	/**
	 * Message queue to buffer incoming messages. The size of the queue is 
	 * determined by MESSAGE_BUFFER_SIZE.
	 */
	private final ArrayBlockingQueue<Message> incomingMessageQueue = new ArrayBlockingQueue<Message>(CONFIGURATIONS.INPUT_MESSAGE_BUFFER_SIZE);
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
				if (server.getSinchanaTestInterface() != null) {
					server.getSinchanaTestInterface().setMessageQueueSize(incomingMessageQueue.size());
				}
				try {
					Message message = incomingMessageQueue.take();
					if (joined) {
						processMessage(message);
						if (waitOnMessageQueueLock) {
							messageQueueLock.release();
						}
					} else {
						switch (message.type) {
							case JOIN:
								if (Arrays.equals(message.source.serverId.array(), serverId)) {
									joined = message.isSuccess();
									server.setJoined(joined, message.getError());
									if (server.getSinchanaTestInterface() != null) {
										server.getSinchanaTestInterface().setStable(joined);
									}
								}
							case DISCOVER_NEIGHBORS:
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
		this.serverIdAsBigInt = server.getServerIdAsBigInt();
		incomingMessageQueueThread.start();
	}

	/**
	 * 
	 * @param message
	 * @return
	 */
	public boolean queueMessage(Message message) {
		if (server.getSinchanaTestInterface() != null) {
			server.getSinchanaTestInterface().incIncomingMessageCount();
			server.getSinchanaTestInterface().setMessageQueueSize(incomingMessageQueue.size());
		}
		synchronized (incomingMessageQueue) {
			return incomingMessageQueue.offer(message);
		}
	}

	public void addRequest(Message message) throws InterruptedException {
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

	/**
	 * 
	 * @param message
	 */
	private synchronized void processMessage(Message message) {

		this.updateTableWithMessage(message);

		switch (message.type) {
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
				this.server.getClientHandler().setResponse(message);
				break;

		}
	}

	private void updateTableWithMessage(Message message) {
		nodes.clear();
		boolean updated = false;
		if (message.isSetFailedNodeSet()) {
			Set<Node> failedNodeSet = message.getFailedNodeSet();
			this.server.getConnectionPool().updateFailedNodeInfo(failedNodeSet, true);
			for (Node node : failedNodeSet) {
				updated = updated || this.server.getRoutingHandler().updateTable(node, false);
			}
		}
		if (!Arrays.equals(message.source.serverId.array(), serverId)) {
			nodes.add(message.source);
		}
		if (!Arrays.equals(message.station.serverId.array(), serverId)) {
			nodes.add(message.station);
		}
		if (message.isSetNeighbourSet()) {
			nodes.addAll(message.getNeighbourSet());
		}
		if (!nodes.isEmpty()) {
			long time = System.currentTimeMillis();
			for (Node node : nodes) {
				if (server.getConnectionPool().hasReportFailed(node)) {
					Connection failedConnection = server.getConnectionPool().getConnection(node);
					if ((failedConnection.getLastHeardFailedTime() + CONFIGURATIONS.FAILED_REACCEPT_TIME_OUT) < time) {
						failedConnection.reset();
					} else {
						continue;
					}
				}
				this.server.getConnectionPool().updateFailedNodeInfo(node, false);
				updated = updated || this.server.getRoutingHandler().updateTable(node, true);
			}
		}
		if (updated) {
			this.server.getRoutingHandler().triggerOptimize();
		}
	}
	private final Set<Node> nodes = new HashSet<Node>();

	private void processRouting(Message message) {
		Node predecessor = this.server.getRoutingHandler().getPredecessors()[0];
		BigInteger targetKeyOffset = getOffset(message.getDestinationId());
		BigInteger predecessorOffset = (predecessor == null ? ZERO : getOffset(predecessor.serverId.array()));
		BigInteger prevStationOffset = getOffset(message.station.serverId.array());

		if (predecessorOffset.compareTo(targetKeyOffset) == -1 || targetKeyOffset.equals(ZERO)) {
			deliverMessage(message);
		} else {
			if (!prevStationOffset.equals(ZERO)
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

	private void processJoin(Message message) {
		if (!Arrays.equals(message.source.serverId.array(), serverId)) {
			if (this.server.getConnectionPool().hasReportFailed(message.source)) {
				Connection connection = this.server.getConnectionPool().getConnection(message.source);
				long remainingTime = Math.max(connection.getLastHeardFailedTime(), connection.getLastKnownFailedTime())
						+ CONFIGURATIONS.FAILED_REACCEPT_TIME_OUT - System.currentTimeMillis();
				if (remainingTime > 0) {
					message.setSuccess(false);
					message.setError(ERROR_MSG_JOIN_REACCEPTANCE + remainingTime);
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
						|| nextSuccessorOffset.equals(ZERO))
						&& tempNodeOffset.compareTo(newServerIdOffset) == -1) {
					nextSuccessor = node;
				}
				if (prevStationIdOffset.compareTo(newServerIdOffset) != 1
						&& (tempNodeOffset.compareTo(nextPredecessorOffset) == -1
						|| nextPredecessorOffset.equals(ZERO))
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

	private void processDiscoverNeighbours(Message message) {
		if (!Arrays.equals(message.source.serverId.array(), serverId)) {
			message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
			this.server.getIOHandler().send(message, message.source);
		}
	}

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
						+ (new String(message.getData()).split(TEST_RING_SEPARATOR).length)
						+ " :: " + new String(message.getData()));
			} else {
				message.setData(this.server.getServerIdAsString().getBytes());
				this.server.getIOHandler().send(message.deepCopy(), predecessor);
				this.server.getIOHandler().send(message.deepCopy(), successor);
			}
		} else {
			if (Arrays.equals(message.station.serverId.array(), predecessor.serverId.array())) {
				message.setData((new String(message.getData()) + TEST_RING_SEPARATOR + this.server.getServerIdAsString()).getBytes());
				this.server.getIOHandler().send(message, successor);
			} else if (Arrays.equals(message.station.serverId.array(), successor.serverId.array())) {
				message.setData((new String(message.getData()) + TEST_RING_SEPARATOR + this.server.getServerIdAsString()).getBytes());
				this.server.getIOHandler().send(message, predecessor);
			} else {
				Logger.log(thisNode, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER, 5,
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
		return ZERO;
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
				handlerAvailable = this.server.getSinchanaRequestHandler() != null;
				if (responseExpected) {
					returnMessage.setType(MessageType.RESPONSE);
					returnMessage.setSuccess(handlerAvailable);
					returnMessage.setLifetime(1 + message.lifetime);
					returnMessage.setRoutedViaPredecessors(message.routedViaPredecessors);
					if (handlerAvailable) {
						returnMessage.setData(this.server.getSinchanaRequestHandler().request(message.getData()));
					} else {
						returnMessage.setError(ERROR_MSG_RESPONSE_HANDLER_NOT_FOUND);
					}
				} else if (handlerAvailable) {
					this.server.getSinchanaRequestHandler().request(message.getData());
				}
				break;
			case STORE_DATA:
				handlerAvailable = this.server.getSinchanaDataStoreInterface() != null;
				if (responseExpected) {
					returnMessage.setType(MessageType.ACKNOWLEDGE_DATA_STORE);
					returnMessage.setSuccess(handlerAvailable
							&& this.server.getSinchanaDataStoreInterface().store(message.getKey(), message.getData()));
					if (!handlerAvailable) {
						returnMessage.setError(ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND);
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
						returnMessage.setError(ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND);
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
						returnMessage.setError(ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND);
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
						returnMessage.setError(ERROR_MSG_SERVICE_HANDLER_NOT_FOUND);
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
