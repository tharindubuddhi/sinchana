/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Set;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import sinchana.util.messagequeue.MessageEventHandler;
import sinchana.util.messagequeue.MessageQueue;

/**
 *
 * @author Hiru
 */
public class MessageHandler {

		private Server server;
		/**
		 * Size of the message buffer.
		 */
		private static final int MESSAGE_BUFFER_SIZE = 2 * RoutingHandler.GRID_SIZE;
		/**
		 * Message queue to buffer incoming messages. The size of the queue is 
		 * determined by MESSAGE_BUFFER_SIZE.
		 */
		private final MessageQueue messageQueue = new MessageQueue(MESSAGE_BUFFER_SIZE, new MessageEventHandler() {

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
						if (!server.getRoutingHandler().isStable()
								&& (message.type != MessageType.JOIN || message.source.serverId != server.serverId)) {
								Logger.log(server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 0,
										"Queued back until the server is stable: " + message.type + " - " + message);
								queueMessage(message);
						} else {
								processMessage(message);
						}
				}
		});

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
		public long init() {
				return messageQueue.getThreadId();
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
						&& (this.server.getRoutingHandler().isStable()
						|| (!this.server.getRoutingHandler().isStable()
						&& message.type == MessageType.JOIN
						&& message.source.serverId == this.server.serverId))) {
						messageQueue.start();
				}

				if (messageQueue.queueMessage(message)) {
						return true;
				} else {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER, 1,
								"Message is unacceptable 'cos buffer is full! " + message);
						return false;
				}
		}

		/**
		 * 
		 * @param message
		 */
		public synchronized void processMessage(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 2,
						"Processing: " + message);

				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node successor = this.server.getRoutingHandler().getSuccessor();

				this.updateTableWithMessage(message);
				if (message.type == MessageType.JOIN) {
						message.setPredecessor(predecessor.deepCopy());
						message.setSuccessor(successor.deepCopy());
				}

				switch (message.type) {
						case GET:
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

						case ACCEPT:
						case ERROR:
								if (this.server.getSinchanaInterface() != null) {
										this.server.getSinchanaInterface().receive(message.deepCopy());
								}
								break;
				}
		}

		private void updateTableWithMessage(Message message) {
				if (message.source.serverId != this.server.serverId) {
						this.server.getRoutingHandler().updateTable(message.source);
				}
				if (message.station.serverId != this.server.serverId
						&& message.station.serverId != message.source.serverId) {
						this.server.getRoutingHandler().updateTable(message.station);
				}
				if (message.isSetPredecessor()
						&& message.predecessor.serverId != this.server.serverId
						&& message.predecessor.serverId != message.source.serverId
						&& message.predecessor.serverId != message.station.serverId) {
						this.server.getRoutingHandler().updateTable(message.predecessor);
				}
				if (message.isSetSuccessor()
						&& message.successor.serverId != this.server.serverId
						&& message.successor.serverId != message.source.serverId
						&& message.successor.serverId != message.station.serverId) {
						this.server.getRoutingHandler().updateTable(message.successor);
				}
				if (message.isSetNeighbourSet()) {
						Set<Node> neighbourSet = message.getNeighbourSet();
						for (Node node : neighbourSet) {
								if (node.serverId != this.server.serverId) {
										this.server.getRoutingHandler().updateTable(node);
								}
						}
				}
		}

		private void processGet(Message message) {
				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node nextHop = this.server.getRoutingHandler().getNextNode(message.targetKey);
				int targetKeyOffset = getOffset(message.targetKey);
				int predecessorOffset = getOffset(predecessor.serverId);
				int prevStationOffset = getOffset(message.station.serverId);
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 3,
						"Routing analyze NH:" + nextHop.serverId + " PD:" + predecessor.serverId + " MSG:" + message);

				if (predecessorOffset < targetKeyOffset || targetKeyOffset == 0) {
						Message returnMessage;
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setStatus("received: " + message.message);
						}
						if (this.server.getSinchanaInterface() != null) {
								returnMessage = this.server.getSinchanaInterface().receive(message.deepCopy());
						} else {
								returnMessage = new Message(this.server, MessageType.ERROR, 1);
								returnMessage.setTargetKey(message.targetKey);
						}
						if (returnMessage != null) {
								returnMessage.setSource(this.server);
								this.server.getPortHandler().send(returnMessage, message.source);
						}
				} else {
						message.message += " " + this.server.serverId;
						if (prevStationOffset != 0 && prevStationOffset < targetKeyOffset) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
										"This should be an errornous receive of " + message.targetKey
										+ " - sending to the predecessor " + predecessor.serverId);
								this.server.getPortHandler().send(message, predecessor);

						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
										"Message is passing to the next node " + nextHop.serverId);
								if (this.server.getSinchanaTestInterface() != null) {
										this.server.getSinchanaTestInterface().setStatus("routed: " + message.message);
								}
								this.server.getPortHandler().send(message, nextHop);
						}
				}
		}

		private void processJoin(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
						"Join message " + message);
				if (message.source.serverId == this.server.serverId) {
						this.server.getRoutingHandler().setStable(true);
				} else {
						int newServerIdOffset = getOffset(message.source.serverId);
						int prevStationIdOffset = getOffset(message.station.serverId);
						int tempNodeOffset, nextPredecessorOffset, nextSuccessorOffset;
						Node nextSuccessor = message.source;
						Node nextPredecessor = message.source;
						Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
						for (Node node : neighbourSet) {
								tempNodeOffset = getOffset(node.serverId);
								nextPredecessorOffset = getOffset(nextPredecessor.serverId);
								nextSuccessorOffset = getOffset(nextSuccessor.serverId);

								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
										"NewNode:" + message.source.serverId
										+ "\tPrevStat:" + message.station.serverId
										+ "\tTempNode:" + node.serverId
										+ "\tnextPDO:" + nextPredecessor.serverId
										+ "\tnextSCO:" + nextSuccessor.serverId);

								if (newServerIdOffset <= prevStationIdOffset
										&& 0 < tempNodeOffset
										&& (nextSuccessorOffset == newServerIdOffset
										|| nextSuccessorOffset < tempNodeOffset)
										&& tempNodeOffset < newServerIdOffset) {
										nextSuccessor = node;
								}
								if (prevStationIdOffset <= newServerIdOffset
										&& 0 < tempNodeOffset
										&& (nextSuccessorOffset == newServerIdOffset
										|| tempNodeOffset < nextPredecessorOffset)
										&& newServerIdOffset < tempNodeOffset) {
										nextPredecessor = node;
								}
						}
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
								"Analyze of state: \tPD: " + nextPredecessor + "\tSC: " + nextSuccessor);

						if (nextPredecessor.serverId == nextSuccessor.serverId) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
										"Sending message to the origin " + message.source.serverId);
								server.getPortHandler().send(message, message.source);
						} else {
								if (nextPredecessor.serverId != message.source.serverId) {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
												"Sending message to the predecessor " + nextPredecessor.serverId);
										server.getPortHandler().send(message, nextPredecessor);
								}
								if (nextSuccessor.serverId != message.source.serverId) {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
												"Sending message to the successor " + nextSuccessor.serverId);
										server.getPortHandler().send(message, nextSuccessor);
								}
						}
				}
		}

		private void processDiscoverNeighbours(Message message) {
				if (message.source.serverId != this.server.serverId) {
						message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
						this.server.getPortHandler().send(message, message.source);
				} else {
						this.server.getRoutingHandler().optimize();
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setStable(true);
						}
				}
		}

		private void processFindSuccessors(Message message) {
				if (message.source.serverId != this.server.serverId) {
						Node newPredecessor = this.server.getRoutingHandler().getOptimalSuccessor(message.getStartOfRange());
						if (newPredecessor.serverId == this.server.serverId) {
								if (message.station.serverId != message.source.serverId) {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 6,
												"Valid response for " + message.targetKey
												+ " in " + message.startOfRange + "-" + message.endOfRange
												+ " is found as " + newPredecessor.serverId
												+ " where source is " + message.source.serverId
												+ " & prevstation is " + message.station.serverId);
										message.setSuccessor(this.server.deepCopy());
										this.server.getPortHandler().send(message, message.source);
								}
						} else {
								this.server.getPortHandler().send(message, newPredecessor);
						}
				}
		}

		private void processTestRing(Message message) {
				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node successor = this.server.getRoutingHandler().getSuccessor();
				if (message.source.serverId == this.server.serverId) {
						if (message.message.length() != 0) {
								Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_MESSAGE_HANDLER, 7,
										"Ring test completed - length: " + (message.message.split(" > ").length) + " :: " + message.message);
								System.out.println("Ring test completed - length: " + (message.message.split(" > ").length) + " :: " + message.message);
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 7,
										"Start of Test Ring. Sending to " + predecessor.serverId + " & " + successor.serverId);
								message.message += "" + this.server.serverId;
								this.server.getPortHandler().send(message, predecessor);
								this.server.getPortHandler().send(message, successor);
						}
				} else {
						if (message.station.serverId == predecessor.serverId) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 7,
										"Forwarding Ring Test to successor " + successor.serverId);
								message.message += " > " + this.server.serverId;
								this.server.getPortHandler().send(message, successor);
						} else if (message.station.serverId == successor.serverId) {
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 7,
										"Forwarding Ring Test to predecessor " + predecessor.serverId);
								message.message += " > " + this.server.serverId;
								this.server.getPortHandler().send(message, predecessor);
						} else {
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER, 7,
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
		private int getOffset(int id) {
				return (id + RoutingHandler.GRID_SIZE - this.server.serverId) % RoutingHandler.GRID_SIZE;
		}
}
