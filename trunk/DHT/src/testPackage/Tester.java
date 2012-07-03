/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package testPackage;

import dhtserverclient.Server;
import dhtserverclient.SinchanaInterface;
import dhtserverclient.SinchanaTestInterface;
import dhtserverclient.chord.FingerTableEntry;
import dhtserverclient.chord.RoutingTable;
import dhtserverclient.thrift.Message;
import dhtserverclient.thrift.MessageType;
import dhtserverclient.thrift.Node;
import java.util.Calendar;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Hiru
 */
public class Tester implements SinchanaInterface, SinchanaTestInterface, Runnable {

		private Server server;
		private int expectedCount = 0;
		private int recievedCount = 0;
		private int resolvedCount = 0;
		private int ringCount = 0;
		private Node anotherNode;
		private int serverId;
		private ServerUI gui = null;
		private TesterController testerController;
		private Calendar startTime;
		private Calendar endTime;
		private Semaphore threadLock = new Semaphore(0);
		private int[] keySpace = new int[RoutingTable.GRID_SIZE];
		private int[] keySpaceReal;

		public Tester(int serverId, Node anotherNode, TesterController tc) {
				server = new Server();
				server.registerSinchanaInterface(this);
				server.registerSinchanaTestInterface(this);
				this.anotherNode = anotherNode;
				this.serverId = serverId;
				this.testerController = tc;
				if (TesterController.GUI_ON) {
						this.gui = new ServerUI(this);
				}
		}
		
		public void startServer(){
				Thread thread = new Thread(this);
				startTime = Calendar.getInstance();
				thread.start();
		}
                public void downServer(){
                                server.stopNode();
                }
		public void startTest() {
				threadLock.release();
		}

		public void startRingTest() {
				Message msg = new Message(this.server, MessageType.TEST_RING, Server.MESSAGE_LIFETIME);
				msg.setMessage("");
				this.server.transferMessage(msg);
		}		

		@Override
		public Message transfer(Message message) {
//				System.out.println(this.server.serverId + ":" + recievedCount + ": Recieved");
				Message response = null;
				switch(message.type){
						case ACCEPT:
								if(keySpaceReal[message.targetKey] != message.source.serverId){
										System.out.println(this.serverId + ": Resolving error : " + message);
								}
								resolvedCount++;
								keySpace[message.getTargetKey()] = message.source.serverId;
								break;
						case ERROR:
								System.out.println(this.serverId + ": Recieved error message : " + message);
								break;
						case GET:
								if(keySpaceReal[message.targetKey] != this.serverId){
										System.out.println(this.serverId + ": Receiving error : " + message);
								}
								recievedCount++;
								response = new Message(this.server, MessageType.ACCEPT, 1);
								response.setTargetKey(message.getTargetKey());
								break;
				}
				endTime = Calendar.getInstance();
				return response;
		}

		@Override
		public void run() {
				try {
						if (anotherNode != null) {
								server.startNode(serverId, serverId + TesterController.LOCAL_PORT_ID_RANGE, "localhost", anotherNode);
						} else {
								server.startNode(serverId, serverId + TesterController.LOCAL_PORT_ID_RANGE, "localhost");
						}
						if (this.gui != null) {
								this.gui.setServerId(serverId);
								this.gui.setVisible(true);
						}
						threadLock.acquire();
						while (ringCount < RoutingTable.GRID_SIZE) {
								Message msg = new Message(this.server, MessageType.GET, RoutingTable.TABLE_SIZE);
								msg.setTargetKey(ringCount);
//								msg.setMessage("Who has " + ringCount + "?");
								this.server.transferMessage(msg);
								ringCount++;
						}
				} catch (InterruptedException ex) {
						Logger.getLogger(Tester.class.getName()).log(Level.SEVERE, null, ex);
				}
		}

		@Override
		public void setStable(boolean isStable) {
				if (isStable) {
						System.out.println(this.server.serverId + ": is now stable!");
						if (this.gui != null) {
								this.gui.setMessage("stabilized!");
						}
						endTime = Calendar.getInstance();
						testerController.incrementCompletedCount();
				}
		}

		@Override
		public void setPredecessor(Node predecessor) {
				if (this.gui != null) {
						this.gui.setPredecessorId(predecessor.serverId);
				}
		}

		@Override
		public void setSuccessor(Node successor) {
				if (this.gui != null) {
						this.gui.setSuccessorId(successor.serverId);
				}
		}

		@Override
		public void setRoutingTable(FingerTableEntry[] fingerTableEntrys) {
				if (this.gui != null) {
						this.gui.setTableInfo(fingerTableEntrys);
				}
		}

		@Override
		public void setStatus(String status) {
				if (this.gui != null) {
						this.gui.setMessage(status);
				}
		}

		public int getExpectedCount() {
				return expectedCount;
		}

		public void setExpectedCount(int expectedCount) {
				this.expectedCount = expectedCount;
		}

		public Calendar getEndTime() {
				return endTime;
		}

		public int getRecievedCount() {
				return recievedCount;
		}

		public int getServerId() {
				return serverId;
		}

		public Calendar getStartTime() {
				return startTime;
		}

		public int getResolvedCount() {
				return resolvedCount;
		}

		public int[] getKeySpace() {
				return keySpace;
		}

		public Server getServer() {
				return server;
		}

		public void setKeySpaceReal(int[] keySpaceReal) {
				this.keySpaceReal = keySpaceReal;
		}
		
		
		
}
