/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dhtserverclient;

import dhtserverclient.chord.RoutingTable;
import dhtserverclient.thrift.Message;
import dhtserverclient.thrift.MessageType;
import dhtserverclient.thrift.Node;

/**
 *
 * @author S.A.H.S.Subasinghe
 */
public class Server extends Node {

		private MessageHandler messageHandler = new MessageHandlerObject(this);
		private PortHandler portHandler = new ThriftServer(this);
		private SinchanaInterface sinchanaInterface = null;
		private SinchanaTestInterface sinchanaTestInterface = null;
		private RoutingHandler routingHandler;
		
		public static final int MESSAGE_LIFETIME = 1024;

		/**
		 * Start a new node with the given server ID. Next hop is not available.
		 * @param id Server ID. Currently it is assigned by the node controller.
		 */
		public void startNode(int serverId, int portId, String address) {
				this.serverId = serverId;
				this.portId = portId;
				this.address = address;
				this.routingHandler = new RoutingTable(this);
				this.startServer();
				if(this.sinchanaTestInterface != null){
						this.sinchanaTestInterface.setStable(true);
				}
		}

		/**
		 * Start a new node with the given server ID and next hop.
		 * @param id Server ID. Currently it is assigned by the node controller.
		 * @param anotherNode Server ID of another node is the network. The new 
		 * node first communicate with this node to discover the rest of the network.
		 */
		public void startNode(int serverId, int portId, String address, Node anotherNode) {
				this.serverId = serverId;
				this.portId = portId;
				this.address = address;
				this.routingHandler = new RoutingTable(this, anotherNode);
				this.startServer();
				Message msg = new Message(this, MessageType.JOIN, MESSAGE_LIFETIME);
				this.portHandler.send(msg, anotherNode.address, anotherNode.portId);
		}
                public void startServer(){
                                this.portHandler.startServer();
                }
                public void stopNode(){
                                portHandler.downServer();
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

		void trace(String message) {
				System.out.println(message);
		}

		public void transferMessage(Message message) {
				message.setSource(this);
				message.setStation(this);
				this.getMessageHandler().queueMessage(message);
		}
		
		//http://cseanremo.appspot.com/remoteip?local=1236&remote=1236
}
