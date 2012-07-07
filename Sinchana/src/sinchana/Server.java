/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

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
		private MessageHandler messageHandler = new MessageHandlerObject(this);
		private SinchanaInterface sinchanaInterface = null;
		private SinchanaTestInterface sinchanaTestInterface = null;
		public static final int MESSAGE_LIFETIME = 1024;
		public long threadId;

		/**
		 * Start a new node with the given server ID. Next hop is not available.
		 * @param id Server ID. Currently it is assigned by the node controller.
		 */
		public Server(int serverId, int portId, String address) {
				this.init(serverId, portId, address);
				this.routingHandler.setStable(true);
		}

		/**
		 * Start a new node with the given server ID and next hop.
		 * @param id Server ID. Currently it is assigned by the node controller.
		 * @param anotherNode Server ID of another node is the network. The new 
		 * node first communicate with this node to discover the rest of the network.
		 */
		public Server(int serverId, int portId, String address, Node anotherNode) {
				this.init(serverId, portId, address);
				Message msg = new Message(this, MessageType.JOIN, MESSAGE_LIFETIME);
				this.portHandler.send(msg, anotherNode);
		}

		private void init(int serverId, int portId, String address) {
				this.serverId = serverId;
				this.portId = portId;
				this.address = address;
				this.routingHandler.init();
				this.threadId = this.messageHandler.init();
				this.startServer();
		}

		public void startServer() {
				this.portHandler.startServer();
		}

		public void stopServer() {
				portHandler.stopServer();
		}

		public void registerSinchanaInterface(SinchanaInterface sinchanaInterface) {
				this.sinchanaInterface = sinchanaInterface;
		}

		public void registerSinchanaTestInterface(SinchanaTestInterface sinchanaTestInterface) {
				this.sinchanaTestInterface = sinchanaTestInterface;
		}

		public MessageHandler getMessageHandler() {
				return messageHandler;
		}

		public PortHandler getPortHandler() {
				return portHandler;
		}

		public RoutingHandler getRoutingHandler() {
				return routingHandler;
		}

		public SinchanaTestInterface getSinchanaTestInterface() {
				return sinchanaTestInterface;
		}

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

		public void transferMessage(Message message) {
				message.setSource(this);
				message.setStation(this);
				this.getMessageHandler().queueMessage(message);
		}
		//http://cseanremo.appspot.com/remoteip?local=1236&remote=1236
}
