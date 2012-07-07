/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.concurrent.Semaphore;
import java.util.logging.Level;
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
public class MessageHandlerObject implements MessageHandler {

		private Server server;
		private static final int MESSAGE_BUFFER_SIZE = 4096;
		private MessageQueue messageQueue;
		private MessageQueue messageQueueUnstable;
		private Semaphore stablePriorty = new Semaphore(0);

		MessageHandlerObject(Server node) {
				this.server = node;
		}

		@Override
		public long init() {
				this.server.getRoutingHandler().updateTable(this.server);
				messageQueueUnstable = new MessageQueue(MESSAGE_BUFFER_SIZE, new MessageEventHandler() {

						@Override
						public void process(Message message) {
								processMessage(message);
								if (messageQueueUnstable.isEmpty()) {
										System.out.println(server.serverId + ": releasing...");
										stablePriorty.release();
								}
						}
				});
				messageQueue = new MessageQueue(MESSAGE_BUFFER_SIZE, new MessageEventHandler() {

						@Override
						public void process(Message message) {
								if (!messageQueueUnstable.isEmpty() && server.getRoutingHandler().isStable()) {
										messageQueueUnstable.start();
										System.out.println(server.serverId + ": waiting.......");
										try {
												stablePriorty.acquire();
										} catch (InterruptedException ex) {
												java.util.logging.Logger.getLogger(MessageHandlerObject.class.getName()).log(Level.SEVERE, null, ex);
										}
								}
								if (!server.getRoutingHandler().isStable()
										&& (message.type != MessageType.JOIN || message.source.serverId != server.serverId)) {
										Logger.log(server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 1,
												"Queued until the server is stable: " + message.type + " - " + message);
										boolean acceptedUnstable = messageQueueUnstable.queueMessage(message);
										if (!acceptedUnstable) {
												Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 8,
														"Message is unacceptable 'cos unstable buffer is full! " + message);
										}
								} else {
										processMessage(message);
								}
						}
				});
				return messageQueue.start();
		}

		@Override
		public boolean queueMessage(Message message) {
				boolean accepted = messageQueue.queueMessage(message);
				if (!accepted) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 8,
								"Message is unacceptable 'cos buffer is full! " + message);
				}
				return accepted;
		}

		public void processMessage(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 0,
						"Processing: " + message);
				Node predecessor = this.server.getRoutingHandler().getPredecessor();
				Node successor = this.server.getRoutingHandler().getSuccessor();

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
												this.server.getPortHandler().send(returnMessage, message.source);
										}
								} else {
										Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 3,
												"Message is passing to the next node " + nextHop.serverId);
										if (this.server.getSinchanaTestInterface() != null) {
												this.server.getSinchanaTestInterface().setStatus("routed: " + message.message);
										}
										this.server.getPortHandler().send(message, nextHop);
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
												server.getPortHandler().send(message, predecessor);
										}
										if (prevStationId != successor.serverId || prevStationId == message.source.serverId) {
												Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 4,
														"Sending message to predecessor " + this.server.getRoutingHandler().getPredecessor().serverId);
												server.getPortHandler().send(message, successor);
										}
								}
								break;
						case DISCOVER_NEIGHBOURS:
								if (message.source.serverId != this.server.serverId) {
										message.setNeighbourSet(this.server.getRoutingHandler().getNeighbourSet());
										this.server.getPortHandler().send(message, message.source);
								} else {
										this.server.getRoutingHandler().setNeighbourSet(message.neighbourSet);
								}
								break;
						case FIND_SUCCESSOR:
								if (message.source.serverId != this.server.serverId) {
										Node newPredecessor = this.server.getRoutingHandler().getOptimalSuccessor(message.source.serverId, message.getStartOfRange());
										if (newPredecessor.serverId == this.server.serverId) {
												message.setSuccessor(this.server.deepCopy());
												this.server.getPortHandler().send(message, message.source);
										} else {
												this.server.getPortHandler().send(message, newPredecessor);
										}

								} else {
										this.server.getRoutingHandler().updateTable(message.getSuccessor());
								}
								break;
						case TEST_RING:
								if (message.source.serverId == this.server.serverId && !(message.message.length() == 0)) {
										Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_MESSAGE_HANDLER_OBJECT, 6,
												"Ring test completed - length: " + (message.message.split(" > ").length - 1) + " :: " + message.message);
										System.out.println("Ring test completed - length: " + (message.message.split(" > ").length - 1) + " :: " + message.message);
								} else {
										message.message += " > " + this.server.serverId;
										this.server.getPortHandler().send(message, successor);
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
}