/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.connection.ThriftServer;
import sinchana.chord.RoutingTable;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;

/**
 *
 * @author S.A.H.S.Subasinghe
 */
public class Server extends Node {

		private PortHandler portHandler = new ThriftServer(this);
		private RoutingHandler routingHandler = new RoutingTable(this);
		private MessageHandler messageHandler = new MessageHandler(this);
		private SinchanaInterface sinchanaInterface = null;
		private SinchanaTestInterface sinchanaTestInterface = null;
		/**
		 * Default life time of a message. At each hop, the lifetime decrements 
		 * and when it reaches 0, the message is discarded.
		 */
		public static final int MESSAGE_LIFETIME = 1024;
		/**
		 * 
		 */
		public long threadId;
		private Node anotherNode;

		/**
		 * Start a new node with the given server ID. Next hop is not available.
		 * @param serverId		Server ID. Generated using a hash function. 
		 * @param portId		Port Id number where the the server is running.
		 * @param address		URL of the server.
		 */
		public Server(int serverId, int portId, String address) {
				this.serverId = serverId;
				this.portId = portId;
				this.address = address;
				this.anotherNode = null;
		}

		/**
		 * Start a new node with the given server ID and next hop.
		 * @param serverId		Server ID. Generated using a hash function. 
		 * @param anotherNode	Another node is the network. New node first
		 * communicate with this node to discover the rest of the network.
		 * @param address		URL of the server.
		 * @param portId		Port Id number where the the server is running.
		 */
		public Server(int serverId, int portId, String address, Node anotherNode) {
				this.serverId = serverId;
				this.portId = portId;
				this.address = address;
				this.anotherNode = anotherNode;
		}

		/**
		 * Start the server.
		 */
		public final void startServer() {
				this.portHandler.startServer();
				this.threadId = Thread.currentThread().getId();
				this.routingHandler.init();
				this.threadId = this.messageHandler.init();
				if (this.anotherNode != null && this.anotherNode.serverId != this.serverId) {
						Message msg = new Message(this, MessageType.JOIN, MESSAGE_LIFETIME);
						this.portHandler.send(msg, this.anotherNode);
				} else {
						this.routingHandler.setStable(true);
				}
		}

		/**
		 * Stop the server.
		 */
		public void stopServer() {
				portHandler.stopServer();
				messageHandler.terminate();
				this.routingHandler.setStable(false);
		}

		/**
		 * Register SinchanaInterface. The callback functions in this interface 
		 * will be called when an event occurs. 
		 * @param sinchanaInterface		SinchanaInterface instance.
		 */
		public void registerSinchanaInterface(SinchanaInterface sinchanaInterface) {
				this.sinchanaInterface = sinchanaInterface;
		}

		/**
		 * Register SinchanaTestInterface. This is only for the testing purposes. 
		 * The callback functions in this interface will be called when an event occurs. 
		 * @param sinchanaTestInterface			SinchanaTestInteface instance.
		 */
		public void registerSinchanaTestInterface(SinchanaTestInterface sinchanaTestInterface) {
				this.sinchanaTestInterface = sinchanaTestInterface;
		}

		/**
		 * 
		 * @return
		 */
		public MessageHandler getMessageHandler() {
				return messageHandler;
		}

		/**
		 * 
		 * @return
		 */
		public PortHandler getPortHandler() {
				return portHandler;
		}

		/**
		 * 
		 * @return
		 */
		public RoutingHandler getRoutingHandler() {
				return routingHandler;
		}

		/**
		 * 
		 * @return
		 */
		public SinchanaTestInterface getSinchanaTestInterface() {
				return sinchanaTestInterface;
		}

		/**
		 * 
		 * @return
		 */
		public SinchanaInterface getSinchanaInterface() {
				return sinchanaInterface;
		}

		/**
		 * Send a message. If the message type is MessageType.GET, targetKey field should be set.
		 * @param message		Message to pass to the network.
		 */
		public void send(Message message) {
				message.setSource(this);
				message.setStation(this);
				this.getMessageHandler().queueMessage(message);
		}

		/**
		 * Send a MessageType.GET message.
		 * @param destination	Destination ID to receive message.
		 * @param message		Message string.
		 */
		public void send(int destination, String message) {
				Message msg = new Message(this, MessageType.GET, MESSAGE_LIFETIME);
				msg.setMessage(message);
				msg.setStation(this);
				this.getMessageHandler().queueMessage(msg);
		}
		//http://cseanremo.appspot.com/remoteip?local=1236&remote=1236
}
