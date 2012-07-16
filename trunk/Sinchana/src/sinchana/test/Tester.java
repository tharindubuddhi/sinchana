/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import sinchana.Server;
import sinchana.SinchanaInterface;
import sinchana.SinchanaTestInterface;
import sinchana.chord.FingerTableEntry;
import sinchana.chord.RoutingTable;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.Calendar;
import java.util.concurrent.Semaphore;

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
		private int serverId;
		private ServerUI gui = null;
		private TesterController testerController;
		private Calendar startTime;
		private Calendar endTime;
		private Semaphore threadLock = new Semaphore(0);
		private int[] keySpace = new int[RoutingTable.GRID_SIZE];
		private int[] realKeySpace;

		/**
		 * 
		 * @param serverId
		 * @param anotherNode
		 * @param tc
		 */
		public Tester(int serverId, Node anotherNode, TesterController tc) {

				server = new Server(
						serverId, serverId + TesterController.LOCAL_PORT_ID_RANGE,
						TesterController.LOCAL_SERVER_ADDRESS, anotherNode);
				server.registerSinchanaInterface(this);
				server.registerSinchanaTestInterface(this);
				this.serverId = serverId;
				this.testerController = tc;
				if (TesterController.GUI_ON) {
						this.gui = new ServerUI(this);
				}
		}

		/**
		 * 
		 */
		public void startServer() {
				Thread thread = new Thread(this);
				startTime = Calendar.getInstance();
				thread.start();
		}

		/**
		 * 
		 */
		public void stopServer() {
				server.stopServer();
		}

		/**
		 * 
		 */
		public void startTest() {
				threadLock.release();
		}

		/**
		 * 
		 */
		public void startRingTest() {
				Message msg = new Message(this.server, MessageType.TEST_RING, Server.MESSAGE_LIFETIME);
				msg.setMessage("");
				this.server.send(msg);
		}

		/**
		 * 
		 */
		public void resetTester() {
				recievedCount = 0;
				resolvedCount = 0;
				ringCount = 0;
		}

		@Override
		public Message receive(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_TESTER, 0,
						"Recieved " + message);
				Message response = null;
				switch (message.type) {
						case ACCEPT:
								if (realKeySpace[message.targetKey] != message.source.serverId) {
										Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 1,
												"Resolving error : " + message);
								} else {
										resolvedCount++;
								}
								keySpace[message.getTargetKey()] = message.source.serverId;
								break;
						case ERROR:
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 2,
										"Recieved error message : " + message);
								break;
						case GET:
								if (realKeySpace[message.targetKey] != this.serverId) {
										Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 3,
												"Receiving error : " + message);
								} else {
										recievedCount++;
										TesterController.incLifeTimeCounter(message.lifetime);
										response = new Message(this.server, MessageType.ACCEPT, 1);
										response.setTargetKey(message.getTargetKey());
								}
								break;
				}
				endTime = Calendar.getInstance();
				return response;
		}

		@Override
		public void run() {
				try {
						if (this.gui != null) {
								this.gui.setServerId(serverId);
								this.gui.setVisible(true);
						}
//						startTime = Calendar.getInstance();
						server.startServer();
						while (true) {
								threadLock.acquire();
								recievedCount = 0;
								resolvedCount = 0;
								ringCount = 0;
								while (ringCount < RoutingTable.GRID_SIZE) {
										Message msg = new Message(this.server, MessageType.GET,
												TesterController.AUTO_TEST_MESSAGE_LIFE_TIME);
										msg.setTargetKey(ringCount);
										msg.setMessage("");
										this.server.send(msg);
										ringCount++;
								}
						}
				} catch (InterruptedException ex) {
						ex.printStackTrace();
				}
		}

		/**
		 * 
		 * @param isStable
		 */
		@Override
		public void setStable(boolean isStable) {
				if (isStable) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 4,
								this.server.serverId + " is now stable!");
						if (this.gui != null) {
								this.gui.setMessage("stabilized!");
						}
						endTime = Calendar.getInstance();
						testerController.incrementCompletedCount(this.serverId);
				}
		}

		/**
		 * 
		 * @param predecessor
		 */
		@Override
		public void setPredecessor(Node predecessor) {
				if (this.gui != null) {
						this.gui.setPredecessorId(predecessor != null ? predecessor.serverId : -1);
				}
		}

		/**
		 * 
		 * @param successor
		 */
		@Override
		public void setSuccessor(Node successor) {
				if (this.gui != null) {
						this.gui.setSuccessorId(successor != null ? successor.serverId : -1);
				}
		}

		/**
		 * 
		 * @param fingerTableEntrys
		 */
		@Override
		public void setRoutingTable(FingerTableEntry[] fingerTableEntrys) {
				if (this.gui != null) {
						this.gui.setTableInfo(fingerTableEntrys);
				}
		}

		/**
		 * 
		 * @param status
		 */
		@Override
		public void setStatus(String status) {
				if (this.gui != null) {
						this.gui.setMessage(status);
				}
		}

		/**
		 * 
		 * @return
		 */
		public int getExpectedCount() {
				return expectedCount;
		}

		/**
		 * 
		 * @param expectedCount
		 */
		public void setExpectedCount(int expectedCount) {
				this.expectedCount = expectedCount;
		}

		/**
		 * 
		 * @return
		 */
		public Calendar getEndTime() {
				return endTime;
		}

		/**
		 * 
		 * @return
		 */
		public int getRecievedCount() {
				return recievedCount;
		}

		/**
		 * 
		 * @return
		 */
		public int getServerId() {
				return serverId;
		}

		/**
		 * 
		 * @return
		 */
		public Calendar getStartTime() {
				return startTime;
		}

		/**
		 * 
		 * @return
		 */
		public int getResolvedCount() {
				return resolvedCount;
		}

		/**
		 * 
		 * @return
		 */
		public int[] getKeySpace() {
				return keySpace;
		}

		/**
		 * 
		 * @return
		 */
		public Server getServer() {
				return server;
		}

		/**
		 * 
		 * @param realKeySpace
		 */
		public void setRealKeySpace(int[] realKeySpace) {
				this.realKeySpace = realKeySpace;
		}

		/**
		 * 
		 * @param isRunning
		 */
		@Override
		public void setServerIsRunning(boolean isRunning) {
				if (this.gui != null) {
						this.gui.setServerRunning(isRunning);
				}
		}

		public void send(int dest, String msg) {
				this.server.send(dest, msg);
		}

		@Override
		public boolean equals(Object obj) {
				return this.serverId == ((Tester) obj).serverId;
		}

		@Override
		public int hashCode() {
				return this.serverId;
		}
}
