/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.service.SinchanaServiceInterface;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import sinchana.connection.Connection;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import sinchana.util.messagequeue.MessageQueue;

/**
 *
 * @author Hiru
 */
public class MessageHandler {

	private final SinchanaServer server;
	private final BigInteger ZERO = new BigInteger("0", CONFIGURATIONS.NUMBER_BASE);
	/**
	 * Message queue to buffer incoming messages. The size of the queue is 
	 * determined by MESSAGE_BUFFER_SIZE.
	 */
	private final MessageQueue messageQueue = new MessageQueue(CONFIGURATIONS.INPUT_MESSAGE_BUFFER_SIZE, new MessageQueue.MessageEventHandler() {

		@Override
		public void process(Message message) {
			/**
			 * A node's join is considered completed if and only if 
			 * it's predecessor and successor are set. Typically, 
			 * receiving at least 1 of 2 MessageType.JOIN messages it 
			 * sends at the beginning is enough to identify and to set 
			 * predecessor and successor. Until the node receives 1 of 
			 * those 2 messages, all the other messages are added back 
			 * to the queue to process later.
			 * Once the node receives it's JOIN message, the joining is 
			 * completed and all the messages are processed with no restriction.
			 */
			if (server.getSinchanaTestInterface() != null) {
				server.getSinchanaTestInterface().setMessageQueueSize(messageQueue.size());
			}
			processMessage(message);

		}
	}, 1);

	/**
	 * Constructor of the class. The server instance is passed as an argument.
	 * @param server SinchanaServer instance. 
	 */
	MessageHandler(SinchanaServer server) {
		this.server = server;
	}

	/**
	 * 
	 */
	public void startAsRootNode() {
		messageQueue.start();
	}

	/**
	 * 
	 */
	public void terminate() {
		messageQueue.reset();
	}

	/**
	 * 
	 * @param message
	 * @return
	 */
	public boolean queueMessage(Message message) {
		if (!messageQueue.isStarted()
				&& message.type == MessageType.JOIN
				&& Arrays.equals(message.source.getServerId(), this.server.getServerId())) {
			messageQueue.start(message);
			this.server.setJoined(true);
			if (this.server.getSinchanaTestInterface() != null) {
				this.server.getSinchanaTestInterface().setStable(true);
			}
			return true;
		} else {
			boolean success = messageQueue.queueMessage(message);
			if (this.server.getSinchanaTestInterface() != null) {
				this.server.getSinchanaTestInterface().incIncomingMessageCount();
				this.server.getSinchanaTestInterface().setMessageQueueSize(messageQueue.size());
			}
			return success;
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
		boolean updated = false;
		if (message.isSetFailedNodeSet()) {
			Set<Node> failedNodeSet = message.getFailedNodeSet();
			this.server.getConnectionPool().updateFailedNodeInfo(failedNodeSet);
			for (Node node : failedNodeSet) {
				updated = updated || this.server.getRoutingHandler().updateTable(node, false);
			}
		}
		Set<Node> nodes = new HashSet<Node>();
		if (!Arrays.equals(message.source.getServerId(), this.server.getServerId())) {
			nodes.add(message.source);
		}
		if (!Arrays.equals(message.station.getServerId(), this.server.getServerId())) {
			nodes.add(message.station);
		}
		if (message.isSetPredecessor() && !Arrays.equals(message.predecessor.getServerId(), this.server.getServerId())) {
			nodes.add(message.getPredecessor());
		}
		if (message.isSetSuccessor() && !Arrays.equals(message.successor.getServerId(), this.server.getServerId())) {
			nodes.add(message.getSuccessor());
		}
		if (message.isSetNeighbourSet()) {
			nodes.addAll(message.getNeighbourSet());
		}
		if (!nodes.isEmpty()) {
			long time = Calendar.getInstance().getTimeInMillis();
			for (Node node : nodes) {
				if (server.getConnectionPool().hasReportFailed(node)) {
					Connection failedConnection = server.getConnectionPool().getConnection(node);
					if ((failedConnection.getLastKnownFailedTime() + CONFIGURATIONS.FAILED_REACCEPT_TIME_OUT) < time) {
						failedConnection.reset();
						System.out.println("Adding back " + node);
					} else {
						System.out.println("Not accepted till "
								+ (failedConnection.getLastKnownFailedTime() + CONFIGURATIONS.FAILED_REACCEPT_TIME_OUT
								- Calendar.getInstance().getTimeInMillis()) + "ms -- " + node);
						continue;
					}
				}
				updated = updated || this.server.getRoutingHandler().updateTable(node, true);
			}
		}
		if (updated) {
			Message msg = new Message(this.server, MessageType.DISCOVER_NEIGHBORS, 2);
			Set<Node> failedNodes = server.getConnectionPool().getFailedNodes();
			msg.setFailedNodeSet(failedNodes);
			Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
			for (Node node : neighbourSet) {
				this.server.getPortHandler().send(msg, node);
			}
		}
	}

	private void processRouting(Message message) {
		Node predecessor = this.server.getRoutingHandler().getPredecessors()[0];
		Node nextHop = this.server.getRoutingHandler().getNextNode(message.getDestinationId());
		BigInteger targetKeyOffset = getOffset(message.getDestinationId());
		BigInteger predecessorOffset = getOffset(predecessor.getServerId());
		BigInteger prevStationOffset = getOffset(message.station.getServerId());

		if (predecessorOffset.compareTo(targetKeyOffset) == -1 || targetKeyOffset.equals(ZERO)) {
			deliverMessage(message);
		} else {
			if (!prevStationOffset.equals(ZERO)
					&& prevStationOffset.compareTo(targetKeyOffset) == -1) {
//				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 3,
//						"This should be an errornous receive of " + message.targetKey
//						+ " - sending to the predecessor " + predecessor.serverId);
				this.server.getPortHandler().send(message, predecessor);

			} else {
				this.server.getPortHandler().send(message, nextHop);
			}
		}
	}

	private void processJoin(Message message) {
		if (!Arrays.equals(message.source.getServerId(), this.server.getServerId())) {
			if (this.server.getConnectionPool().hasReportFailed(message.source)) {
				Logger.log(this.server, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER, 4,
						"Node " + message.source + "has to wait.");
				return;
			}
			message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());

			BigInteger newServerIdOffset = getOffset(message.source.getServerId());
			BigInteger prevStationIdOffset = getOffset(message.station.getServerId());
			BigInteger tempNodeOffset, nextPredecessorOffset, nextSuccessorOffset;
			Node nextSuccessor = this.server;
			Node nextPredecessor = this.server;
			Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
			for (Node node : neighbourSet) {
				tempNodeOffset = getOffset(node.getServerId());
				nextPredecessorOffset = getOffset(nextPredecessor.getServerId());
				nextSuccessorOffset = getOffset(nextSuccessor.getServerId());

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
			if (!Arrays.equals(nextPredecessor.getServerId(), this.server.getServerId())) {
				server.getPortHandler().send(message, nextPredecessor);
			} else {
				server.getPortHandler().send(message, message.source);
			}
			if (!Arrays.equals(nextSuccessor.getServerId(), this.server.getServerId())) {
				server.getPortHandler().send(message, nextSuccessor);
			} else {
				server.getPortHandler().send(message, message.source);
			}
		}
	}

	private void processDiscoverNeighbours(Message message) {
		if (!Arrays.equals(message.source.getServerId(), this.server.getServerId())) {
			message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
			this.server.getPortHandler().send(message, message.source);
		}
	}

	private void processTestRing(Message message) {
		Node predecessor = this.server.getRoutingHandler().getPredecessors()[0];
		Node successor = this.server.getRoutingHandler().getSuccessors()[0];
		if (Arrays.equals(message.source.getServerId(), this.server.getServerId())) {
			if (message.isSetData()) {
				System.out.println("Ring test completed - length: "
						+ (new String(message.getData()).split(" > ").length)
						+ " :: " + new String(message.getData()));
			} else {
				message.setData(this.server.getServerIdAsString().getBytes());
				this.server.getPortHandler().send(message, predecessor);
				this.server.getPortHandler().send(message, successor);
			}
		} else {
			if (Arrays.equals(message.station.getServerId(), predecessor.getServerId())) {
				message.setData((new String(message.getData()) + " > " + this.server.getServerIdAsString()).getBytes());
				this.server.getPortHandler().send(message, successor);
			} else if (Arrays.equals(message.station.getServerId(), successor.getServerId())) {
				message.setData((new String(message.getData()) + " > " + this.server.getServerIdAsString()).getBytes());
				this.server.getPortHandler().send(message, predecessor);
			} else {
				Logger.log(this.server, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER, 5,
						"Message Terminated! Received from " + message.station.serverId
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
		return SinchanaServer.GRID_SIZE.add(new BigInteger(id)).subtract(server.getServerIdAsBigInt()).mod(SinchanaServer.GRID_SIZE);
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
			returnMessage.setSource(server);
			returnMessage.setLifetime(CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
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
					if (handlerAvailable) {
						returnMessage.setData(this.server.getSinchanaRequestHandler().request(message.getData()));
					} else {
						returnMessage.setError("Request handler is not found!");
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
						returnMessage.setError("Data store is not found!");
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
						returnMessage.setError("Data store is not found!");
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
						returnMessage.setError("Data store is not found!");
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
						returnMessage.setError("Service is not found!");
					}
				} else if (handlerAvailable) {
					ssi.process(message.getKey(), message.getData());
				}
				break;
		}
		if (responseExpected) {
			this.server.getPortHandler().send(returnMessage, returnMessage.destination);
		}
	}
}
