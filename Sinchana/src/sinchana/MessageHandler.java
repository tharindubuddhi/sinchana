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
		private static final int MESSAGE_BUFFER_SIZE = 1536;
		
		private MessageQueue messageQueue = new MessageQueue(MESSAGE_BUFFER_SIZE, new MessageEventHandler() {

				@Override
				public void process(Message message) {
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

		MessageHandler(Server node) {
				this.server = node;
		}

		public long init() {
				return messageQueue.start();
		}
		
		public void terminate(){
				messageQueue.reset();
		}

		public boolean queueMessage(Message message) {
				boolean accepted = messageQueue.queueMessage(message);
				if (!accepted) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER, 1,
								"Message is unacceptable 'cos buffer is full! " + message);
				}
				return accepted;
		}

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

				predecessor = this.server.getRoutingHandler().getPredecessor();
				successor = this.server.getRoutingHandler().getSuccessor();

				switch (message.type) {
						case GET:
								Node nextHop = this.server.getRoutingHandler().getNextNode(message.targetKey);
								int thisServerOffset = (this.server.serverId + RoutingHandler.GRID_SIZE - message.targetKey) % RoutingHandler.GRID_SIZE;
								int predecessorOffset = (predecessor.serverId + RoutingHandler.GRID_SIZE - message.targetKey) % RoutingHandler.GRID_SIZE;
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 3,
										"Routing analyze NH:" + nextHop.serverId + " PD:" + predecessor.serverId + " MSG:" + message);

								if (thisServerOffset <= predecessorOffset) {
										Message returnMessage;
										if (this.server.getSinchanaTestInterface() != null) {
												this.server.getSinchanaTestInterface().setStatus("received: " + message.message);
										}
										if (this.server.getSinchanaInterface() != null) {
												returnMessage = this.server.getSinchanaInterface().transfer(message.deepCopy());
												returnMessage.setSource(this.server);
										} else {
												returnMessage = new Message(this.server, MessageType.ERROR, 1);
												returnMessage.setTargetKey(message.targetKey);
										}
										if (returnMessage != null) {
												this.server.getPortHandler().send(returnMessage, message.source);
										}
								} else {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 4,
												"Message is passing to the next node " + nextHop.serverId);
										if (this.server.getSinchanaTestInterface() != null) {
												this.server.getSinchanaTestInterface().setStatus("routed: " + message.message);
										}
										this.server.getPortHandler().send(message, nextHop);
								}
								break;
						case JOIN:
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER, 5,
										"Join message " + message);
								if (message.source.serverId == this.server.serverId) {
										this.server.getRoutingHandler().setStable(true);
								} else {
										int newServerIdOffset = (message.source.serverId + RoutingHandler.GRID_SIZE - this.server.serverId)
												% RoutingHandler.GRID_SIZE;
										int prevStationIdOffset = (message.station.serverId + RoutingHandler.GRID_SIZE - this.server.serverId)
												% RoutingHandler.GRID_SIZE;
										int tempNodeOffset, nextPredecessorOffset, nextSuccessorOffset;
										Node nextSuccessor = message.source;
										Node nextPredecessor = message.source;
										Set<Node> neighbourSet = this.server.getRoutingHandler().getNeighbourSet();
										for (Node node : neighbourSet) {
												tempNodeOffset = (node.serverId + RoutingHandler.GRID_SIZE - this.server.serverId)
														% RoutingHandler.GRID_SIZE;
												nextPredecessorOffset = (nextPredecessor.serverId + RoutingHandler.GRID_SIZE - this.server.serverId)
														% RoutingHandler.GRID_SIZE;
												nextSuccessorOffset = (nextSuccessor.serverId + RoutingHandler.GRID_SIZE - this.server.serverId)
														% RoutingHandler.GRID_SIZE;

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
								break;
						case DISCOVER_NEIGHBOURS:
								if (message.source.serverId != this.server.serverId) {
										message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
										this.server.getPortHandler().send(message, message.source);
								} else {
										this.server.getRoutingHandler().setNeighbourSet(message.neighbourSet);
										if (this.server.getSinchanaTestInterface() != null) {
												this.server.getSinchanaTestInterface().setStable(true);
										}
								}
								break;
						case FIND_SUCCESSOR:
								if (message.source.serverId != this.server.serverId) {
										Node newPredecessor = this.server.getRoutingHandler().getOptimalSuccessor(message.source.serverId, message.getStartOfRange());
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
								break;


						case TEST_RING:
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
								break;
						case ACCEPT:
						case ERROR:
								if (this.server.getSinchanaInterface() != null) {
										this.server.getSinchanaInterface().transfer(message.deepCopy());
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
		}
}
