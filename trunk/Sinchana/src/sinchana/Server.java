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
		 * @param anotherNode	Server ID of another node is the network. The new 
		 *	node first communicate with this node to discover the rest of the network.
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
				if (this.anotherNode != null) {
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

		void printState() {
				System.out.print("Server " + this.serverId + " @ " + this.address + ":" + portId);
				if (this.routingHandler.getPredecessor() != null) {
						System.out.print(" P:" + this.routingHandler.getPredecessor().serverId);
				}
				if (this.routingHandler.getSuccessor() != null) {
						System.out.print(" S:" + this.routingHandler.getSuccessor().serverId);
				}
				System.out.println("");
		}

		/**
		 * 
		 * @param message
		 */
		public void transferMessage(Message message) {
				message.setSource(this);
				message.setStation(this);
				this.getMessageHandler().queueMessage(message);
		}
		//http://cseanremo.appspot.com/remoteip?local=1236&remote=1236
}
