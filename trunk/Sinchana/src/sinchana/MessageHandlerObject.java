/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import java.util.concurrent.Semaphore;
import sinchana.util.logging.Logger;

/**
 *
 * @author Hiru
 */
public class MessageHandlerObject implements MessageHandler, Runnable {

		private Server server;
		private static final int MESSAGE_BUFFER_SIZE = 3072;
		private int head = 0;
		private int tail = 0;
		private Message[] messageQueue = new Message[MESSAGE_BUFFER_SIZE];
		private Semaphore messagesAvailable = new Semaphore(0);
		private static final int JOIN_MESSAGE_BUFFER_SIZE = 192;
		private int jHead = 0;
		private int jTail = 0;
		private Message[] jMessageQueue = new Message[JOIN_MESSAGE_BUFFER_SIZE];

		MessageHandlerObject(Server node) {
				this.server = node;
		}

		@Override
		public long init() {
				Thread thread = new Thread(this);
				thread.start();
				return thread.getId();
		}

		public void processMessage(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 0,
						"Processing: " + message);
				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node successor = this.server.getRoutingHandler().getSuccessor();

				if (!this.server.getRoutingHandler().isStable()
						&& (message.type != MessageType.JOIN || message.source.serverId != this.server.serverId)) {
						Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 1,
								"Queued until server is stable: " + message.type + " - " + message);
						queueJoinMessage(message);
						return;
				}

//				this.server.getRoutingHandler().updateTable(message.source);
//				if (message.source.serverId != message.station.serverId) {
//						this.server.getRoutingHandler().updateTable(message.station);
//				}

				switch (message.type) {
						case GET:
								Node nextHop = this.server.getRoutingHandler().getNextNode(message.targetKey);
								int thisServerOffset = (this.server.serverId + RoutingHandler.GRID_SIZE - message.targetKey) % RoutingHandler.GRID_SIZE;
								int predecessorOffset = (predecessor.serverId + RoutingHandler.GRID_SIZE - message.targetKey) % RoutingHandler.GRID_SIZE;
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 2,
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
												this.server.getPortHandler().send(returnMessage, message.source.address, message.source.portId);
										}
								} else {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 3,
												"Message is passing to the next node " + nextHop.serverId);
										if (this.server.getSinchanaTestInterface() != null) {
												this.server.getSinchanaTestInterface().setStatus("routed: " + message.message);
										}
										this.server.getPortHandler().send(message, nextHop.address, nextHop.portId);
								}
								break;
						case JOIN:
								Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 4,
										"Join message " + message);
								if (message.source.serverId == this.server.serverId) {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 4,
												"Updating table with " + message.station.serverId + " "
												+ message.predecessor.serverId + " & " + message.successor.serverId);
										this.server.getRoutingHandler().updateTable(message.station);
										this.server.getRoutingHandler().updateTable(message.predecessor);
										this.server.getRoutingHandler().updateTable(message.successor);
										this.server.getRoutingHandler().setStable(true);
								} else {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 4,
												"Updating table with " + message.source.serverId);
										message.setPredecessor(this.server.getRoutingHandler().getPredecessor().deepCopy());
										message.setSuccessor(this.server.getRoutingHandler().getSuccessor().deepCopy());
										this.server.getRoutingHandler().updateTable(message.source);
										predecessor = this.server.getRoutingHandler().getPredecessor();
										successor = this.server.getRoutingHandler().getSuccessor();
										int prevStationId = message.station.serverId;
										if (prevStationId != predecessor.serverId || prevStationId == message.source.serverId) {
												Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 4,
														"Sending message to successor " + successor.serverId);
												server.getPortHandler().send(message, predecessor.address, predecessor.portId);
										}
										if (prevStationId != successor.serverId || prevStationId == message.source.serverId) {
												Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 4,
														"Sending message to predecessor " + this.server.getRoutingHandler().getPredecessor().serverId);
												server.getPortHandler().send(message, successor.address, successor.portId);
										}
								}
								break;
						case DISCOVER_NEIGHBOURS:
								if (message.source.serverId != this.server.serverId) {
										message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
										this.server.getPortHandler().send(message, message.source.address, message.source.portId);
								} else {
										this.server.getRoutingHandler().setNeighbourSet(message.neighbourSet);
								}
								break;
						case FIND_SUCCESSOR:
								if (message.source.serverId != this.server.serverId) {
										Node newPredecessor = this.server.getRoutingHandler().getOptimalSuccessor(message.source.serverId, message.getStartOfRange());
										if (newPredecessor.serverId == this.server.serverId) {
												message.setSuccessor(this.server.deepCopy());
												this.server.getPortHandler().send(message, message.source.address, message.source.portId);
										} else {
												this.server.getPortHandler().send(message, newPredecessor.address, newPredecessor.portId);
										}

								} else {
										this.server.getRoutingHandler().setOptimalSuccessor(message.getStartOfRange(), message.getSuccessor());
								}
								break;
						case TEST_RING:
								if (message.source.serverId == this.server.serverId && !(message.message.length() == 0)) {
										Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 6,
												"Ring test completed - length: " + (message.message.split(" > ").length - 1) + " :: " + message.message);
										System.out.println("Ring test completed - length: " + (message.message.split(" > ").length - 1) + " :: " + message.message);
								} else {
										message.message += " > " + this.server.serverId;
										this.server.getPortHandler().send(message,
												successor.address, successor.portId);
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

		@Override
		public synchronized boolean queueMessage(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 7,
						"Queued " + message);
				if ((tail + MESSAGE_BUFFER_SIZE - head) % MESSAGE_BUFFER_SIZE == 1) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 8,
								"Message is unacceptable 'cos buffer is full! " + message);
						return false;
//						System.exit(1);
				}
				messageQueue[head] = message;
				head = (head + 1) % MESSAGE_BUFFER_SIZE;
				messagesAvailable.release();
				return true;
		}

		private synchronized boolean queueJoinMessage(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 9,
						"Queued to join " + message);
				if ((jTail + JOIN_MESSAGE_BUFFER_SIZE - jHead) % JOIN_MESSAGE_BUFFER_SIZE == 1) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 10,
								"Join message is unacceptable 'cos buffer is full! " + message);
//						System.exit(1);
						return false;
				}
				jMessageQueue[jHead] = message;
				jHead = (jHead + 1) % JOIN_MESSAGE_BUFFER_SIZE;
				return true;
		}

		@Override
		public void run() {
				this.server.getRoutingHandler().updateTable(this.server);
				while (true) {
						try {
								if (jHead > jTail && this.server.getRoutingHandler().isStable()) {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 11,
												"Processing from queue " + jMessageQueue[jTail]);
										processMessage(jMessageQueue[jTail]);
										jTail = (jTail + 1) % JOIN_MESSAGE_BUFFER_SIZE;
								} else {
										messagesAvailable.acquire();
										processMessage(messageQueue[tail]);
										tail = (tail + 1) % MESSAGE_BUFFER_SIZE;
								}
						} catch (InterruptedException ex) {
								ex.printStackTrace();
						}
				}
		}
}
