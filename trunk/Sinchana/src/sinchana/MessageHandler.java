/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.math.BigInteger;
import java.util.Set;
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

		private Server server;
		/**
		 * Message queue to buffer incoming messages. The size of the queue is 
		 * determined by MESSAGE_BUFFER_SIZE.
		 */
		private final MessageQueue messageQueue = new MessageQueue(CONFIG.INPUT_MESSAGE_BUFFER_SIZE, new MessageQueue.MessageEventHandler() {

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
		 * @param server Server instance. 
		 */
		MessageHandler(Server server) {
				this.server = server;
		}

		/**
		 * Initializes message handler.
		 * @return thread id of the message queue.
		 */
		public long[] init() {
				return messageQueue.getThreadIds();
		}

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
		public synchronized boolean queueMessage(Message message) {
				if (!messageQueue.isStarted()
						&& message.type == MessageType.JOIN
						&& message.source.serverId.equals(this.server.serverId)) {
						processMessage(message);
						messageQueue.start();
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_MESSAGE_HANDLER, 0,
								"Message Queue is started");
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setStable(true);
						}
						return true;
				} else {

						if (messageQueue.queueMessage(message)) {
								if (this.server.getSinchanaTestInterface() != null) {
										this.server.getSinchanaTestInterface().incIncomingMessageCount();
										this.server.getSinchanaTestInterface().setMessageQueueSize(messageQueue.size());
								}
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 0,
										"Queued: " + message);
								return true;
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_MESSAGE_HANDLER, 0,
										"Message is unacceptable 'cos buffer is full! " + message);
								return false;
						}
				}
		}

		/**
		 * 
		 * @param message
		 */
		private synchronized void processMessage(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 1,
						"Processing: " + message);

				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node successor = this.server.getRoutingHandler().getSuccessor();

				this.updateTableWithMessage(message);
				if (message.type == MessageType.JOIN) {
						message.setPredecessor(predecessor.deepCopy());
						message.setSuccessor(successor.deepCopy());
				}

				switch (message.type) {
						case REQUEST:
						case STORE_DATA:
						case DELETE_DATA:
						case GET_DATA:
						case PUBLISH_SERVICE:
						case GET_SERVICE:
						case REMOVE_SERVICE:
								this.processGet(message);
								break;
						case JOIN:
								this.processJoin(message);
								break;
						case DISCOVER_NEIGHBOURS:
								this.processDiscoverNeighbours(message);
								break;
						case FIND_SUCCESSOR:
								this.processFindSuccessors(message);
								break;
						case TEST_RING:
								this.processTestRing(message);
								break;

						case ERROR:
						case RESPONSE:
						case RESPONSE_DATA:
						case RESPONSE_SERVICE:
						case FAILURE_DATA:
						case FAILURE_SERVICE:
						case ACKNOWLEDGE_DATA:
						case ACKNOWLEDGE_SERVICE:
								deliverMessage(message);
								break;
				}
		}

		private void updateTableWithMessage(Message message) {
				if (!message.source.serverId.equals(this.server.serverId)) {
						this.server.getRoutingHandler().updateTable(message.source);
				}
				if (!message.station.serverId.equals(this.server.serverId)
						&& !message.station.serverId.equals(message.source.serverId)) {
						this.server.getRoutingHandler().updateTable(message.station);
				}
				if (message.isSetPredecessor()
						&& !message.predecessor.serverId.equals(this.server.serverId)
						&& !message.predecessor.serverId.equals(message.source.serverId)
						&& !message.predecessor.serverId.equals(message.station.serverId)) {
						this.server.getRoutingHandler().updateTable(message.predecessor);
				}
				if (message.isSetSuccessor()
						&& !message.successor.serverId.equals(this.server.serverId)
						&& !message.successor.serverId.equals(message.source.serverId)
						&& !message.successor.serverId.equals(message.station.serverId)) {
						this.server.getRoutingHandler().updateTable(message.successor);
				}
				if (message.isSetNeighbourSet()) {
						Set<Node> neighbourSet = message.getNeighbourSet();
						for (Node node : neighbourSet) {
								if (!this.server.serverId.equals(node.serverId)) {
										this.server.getRoutingHandler().updateTable(node);
								}
						}
				}
		}

		private void processGet(Message message) {
				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node nextHop = this.server.getRoutingHandler().getNextNode(message.targetKey);
				BigInteger targetKeyOffset = getOffset(message.targetKey);
				BigInteger predecessorOffset = getOffset(predecessor.serverId);
				BigInteger prevStationOffset = getOffset(message.station.serverId);

				if (predecessorOffset.compareTo(targetKeyOffset) == -1 || targetKeyOffset.equals(new BigInteger("0", 16))) {
						deliverMessage(message);
				} else {
						message.message += " " + this.server.serverId;
						if (!prevStationOffset.equals(new BigInteger("0", 16))
								&& prevStationOffset.compareTo(targetKeyOffset) == -1) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 3,
										"This should be an errornous receive of " + message.targetKey
										+ " - sending to the predecessor " + predecessor.serverId);
								this.server.getPortHandler().send(message, predecessor);

						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 3,
										"Message is passing to the next node " + nextHop.serverId);
								if (this.server.getSinchanaTestInterface() != null) {
										this.server.getSinchanaTestInterface().setStatus("routed: " + message.message);
								}
								this.server.getPortHandler().send(message, nextHop);
						}
				}
		}

		private void processJoin(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
						"Join message " + message);
				if (!message.source.serverId.equals(this.server.serverId)) {
						BigInteger newServerIdOffset = getOffset(message.source.serverId);
						BigInteger prevStationIdOffset = getOffset(message.station.serverId);
						BigInteger tempNodeOffset, nextPredecessorOffset, nextSuccessorOffset;
						Node nextSuccessor = this.server;
						Node nextPredecessor = this.server;
						Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
						for (Node node : neighbourSet) {
								tempNodeOffset = getOffset(node.serverId);
								nextPredecessorOffset = getOffset(nextPredecessor.serverId);
								nextSuccessorOffset = getOffset(nextSuccessor.serverId);

								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
										"NewNode:" + message.source.serverId
										+ "\tPrevStat:" + message.station.serverId
										+ "\tTempNode:" + node.serverId
										+ "\tnextPDO:" + nextPredecessor.serverId
										+ "\tnextSCO:" + nextSuccessor.serverId);

								if (newServerIdOffset.compareTo(prevStationIdOffset) != 1
										&& (nextSuccessorOffset.compareTo(tempNodeOffset) == -1
										|| nextSuccessorOffset.equals(new BigInteger("0", 16)))
										&& tempNodeOffset.compareTo(newServerIdOffset) == -1) {
										nextSuccessor = node;
								}
								if (prevStationIdOffset.compareTo(newServerIdOffset) != 1
										&& (tempNodeOffset.compareTo(nextPredecessorOffset) == -1
										|| nextPredecessorOffset.equals(new BigInteger("0", 16)))
										&& newServerIdOffset.compareTo(tempNodeOffset) == -1) {
										nextPredecessor = node;
								}
						}
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
								"Analyze of state: \tPD: " + nextPredecessor + "\tSC: " + nextSuccessor);

						if (!nextPredecessor.serverId.equals(this.server.serverId)) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
										"Sending message to the predecessor " + nextPredecessor.serverId);
								server.getPortHandler().send(message, nextPredecessor);
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
										"Sending message to the origin " + message.source.serverId);
								server.getPortHandler().send(message, message.source);
						}
						if (!nextSuccessor.serverId.equals(this.server.serverId)) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
										"Sending message to the successor " + nextSuccessor.serverId);
								server.getPortHandler().send(message, nextSuccessor);
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
										"Sending message to the origin " + message.source.serverId);
								server.getPortHandler().send(message, message.source);
						}

				}
		}

		private void processDiscoverNeighbours(Message message) {
				if (!message.source.serverId.equals(this.server.serverId)) {
						message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
						this.server.getPortHandler().send(message, message.source);
				} else {
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 6,
								"Receving NeighbourSet from " + message.station.serverId);
				}
		}

		private void processFindSuccessors(Message message) {
				if (!message.source.serverId.equals(this.server.serverId)) {
						this.server.getRoutingHandler().getOptimalSuccessor(message);
				}
		}

		private void processTestRing(Message message) {
				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node successor = this.server.getRoutingHandler().getSuccessor();
				if (message.source.serverId.equals(this.server.serverId)) {
						if (message.message.length() != 0) {
								Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_MESSAGE_HANDLER, 5,
										"Ring test completed - length: " + (message.message.split(" > ").length) + " :: " + message.message);
								System.out.println("Ring test completed - length: " + (message.message.split(" > ").length) + " :: " + message.message);
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
										"Start of Test Ring. Sending to " + predecessor.serverId + " & " + successor.serverId);
								message.message += "" + this.server.serverId;
								this.server.getPortHandler().send(message, predecessor);
								this.server.getPortHandler().send(message, successor);
						}
				} else {
						if (message.station.serverId.equals(predecessor.serverId)) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
										"Forwarding Ring Test to successor " + successor.serverId);
								message.message += " > " + this.server.serverId;
								this.server.getPortHandler().send(message, successor);
						} else if (message.station.serverId.equals(successor.serverId)) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
										"Forwarding Ring Test to predecessor " + predecessor.serverId);
								message.message += " > " + this.server.serverId;
								this.server.getPortHandler().send(message, predecessor);
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER, 5,
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
		private BigInteger getOffset(String id) {
				return Server.GRID_SIZE.add(new BigInteger(id, 16)).subtract(server.getServerIdAsBigInt()).mod(Server.GRID_SIZE);
		}

		private void deliverMessage(Message message) {
				Message returnMessage = null;
				boolean success = false;
				switch (message.type) {
						case REQUEST:
								if (this.server.getSinchanaInterface() != null) {
										returnMessage = this.server.getSinchanaInterface().request(message.deepCopy());
								}
								break;
						case RESPONSE:
								if (this.server.getSinchanaInterface() != null) {
										this.server.getSinchanaInterface().response(message.deepCopy());
								}
								break;
						case ERROR:
								if (this.server.getSinchanaInterface() != null) {
										this.server.getSinchanaInterface().error(message.deepCopy());
								}
								break;

						case STORE_DATA:
								if (this.server.getSinchanaStoreInterface() != null) {
										success = this.server.getSinchanaStoreInterface().store(message.targetKey, message.message);
								}
								break;
						case DELETE_DATA:
								if (this.server.getSinchanaStoreInterface() != null) {
										success = this.server.getSinchanaStoreInterface().delete(message.targetKey);
								}
								break;
						case GET_DATA:
								if (this.server.getSinchanaStoreInterface() != null) {
										returnMessage = new Message();
										returnMessage.setLifetime(1);
										message.setType(MessageType.RESPONSE_DATA);
										message.setMessage(this.server.getSinchanaStoreInterface().get(message.targetKey));
								}
								break;
						case FAILURE_DATA:
								break;
						case ACKNOWLEDGE_DATA:
								break;
						case RESPONSE_DATA:
								break;

						case PUBLISH_SERVICE:
								if (this.server.getSinchanaServiceInterface() != null) {
										success = this.server.getSinchanaServiceInterface().publish(message.targetKey, message.message);
								}
								break;
						case REMOVE_SERVICE:
								if (this.server.getSinchanaServiceInterface() != null) {
										success = this.server.getSinchanaServiceInterface().remove(message.targetKey);
								}
								break;
						case GET_SERVICE:
								if (this.server.getSinchanaServiceInterface() != null) {
										returnMessage = new Message();
										returnMessage.setLifetime(1);
										message.setType(MessageType.RESPONSE_SERVICE);
										message.setMessage(this.server.getSinchanaServiceInterface().get(message.targetKey));
								}
								break;
						case FAILURE_SERVICE:
								break;
						case ACKNOWLEDGE_SERVICE:
								break;
						case RESPONSE_SERVICE:
								break;
				}
				if (returnMessage != null) {
						returnMessage.setSource(this.server);
						this.server.getPortHandler().send(returnMessage, message.source);
				} else if (success) {
				}
		}
}
