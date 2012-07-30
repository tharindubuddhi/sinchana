/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLConnection;
import sinchana.Server;
import sinchana.SinchanaInterface;
import sinchana.SinchanaTestInterface;
import sinchana.chord.FingerTableEntry;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

/**
 *
 * @author Hiru
 */
public class Tester implements SinchanaInterface, SinchanaTestInterface, Runnable {

		private Server server;
		private long expectedCount = 0;
		private long recievedCount = 0;
		private long resolvedCount = 0;
		private int lifeTimeCount = 0;
		private int ringCount = 0;
		private short testId;
		private ServerUI gui = null;
		private TesterController testerController;
		private Calendar startTime;
		private Calendar endTime;
		private Semaphore threadLock = new Semaphore(0);
		private Map<Long, Long> keySpace = new HashMap<Long, Long>();
		private Map<Long, Long> realKeySpace;
		private boolean running = false;

		/**
		 * 
		 * @param serverId
		 * @param anotherNode
		 * @param tc
		 */
		public Tester(short testId, TesterController tc) {

				this.testId = testId;
				this.testerController = tc;
				server = new Server(
						(short) (testId + TesterController.LOCAL_PORT_ID_RANGE));
				Node node = getRemoteNode(server.serverId,
						server.address, server.portId);
				server.setAnotherNode(node);
				server.registerSinchanaInterface(this);
				server.registerSinchanaTestInterface(this);
				server.startServer();
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
				this.running = true;
		}

		/**
		 * 
		 */
		public void stopServer() {
				server.stopServer();
				this.running = false;
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
				lifeTimeCount = 0;
		}

		@Override
		public Message receive(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_TESTER, 0,
						"Recieved " + message);
				Message response = null;
				switch (message.type) {
						case ACCEPT:
//								if (realKeySpace[message.targetKey] != message.source.serverId) {
//										Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 1,
//												"Resolving error : " + message);
//								} else {
//										resolvedCount++;
//								}
//								keySpace[message.getTargetKey()] = message.source.serverId;
								break;
						case ERROR:
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 2,
										"Recieved error message : " + message);
								break;
						case GET:
//								if (realKeySpace[message.targetKey] != this.server.serverId) {
//										Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 3,
//												"Receiving error : " + message);
//								} else {
//										recievedCount++;
//										lifeTimeCount += message.lifetime;
//										response = new Message(this.server, MessageType.ACCEPT, 1);
//										response.setTargetKey(message.getTargetKey());
//								}
								break;
				}
				endTime = Calendar.getInstance();
				return response;
		}

		@Override
		public void run() {
				try {
						if (this.gui != null) {
								this.gui.setServerId(server.serverId);
								this.gui.setVisible(true);
						}
//						startTime = Calendar.getInstance();
						server.join();
						while (true) {
								threadLock.acquire();
								resetTester();
								while (ringCount < Server.GRID_SIZE) {
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
						testerController.incrementCompletedCount(this.testId);
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
		public long getExpectedCount() {
				return expectedCount;
		}

		/**
		 * 
		 * @param expectedCount
		 */
		public void setExpectedCount(long expectedCount) {
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
		public long getRecievedCount() {
				return recievedCount;
		}

		/**
		 * 
		 * @return
		 */
		public long getServerId() {
				return server.serverId;
		}

		public int getTestId() {
				return testId;
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
		public long getResolvedCount() {
				return resolvedCount;
		}

		public int getLifeTimeCount() {
				return lifeTimeCount;
		}

		/**
		 * 
		 * @return
		 */
		public Map<Long, Long> getKeySpace() {
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
		public void setRealKeySpace(Map<Long, Long> realKeySpace) {
				this.realKeySpace = realKeySpace;
		}

		public boolean isRunning() {
				return running;
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
				return this.testId == ((Tester) obj).testId;
		}

		@Override
		public int hashCode() {
				return (int) this.server.serverId;
		}

		private Node getRemoteNode(long serverId, String address, int portId) {
				try {
						URL url = new URL("http://cseanremo.appspot.com/remoteip?"
								+ "sid=" + serverId
								+ "&url=" + address
								+ "&pid=" + portId);
						URLConnection yc = url.openConnection();
						InputStreamReader isr = new InputStreamReader(yc.getInputStream());
						BufferedReader in = new BufferedReader(isr);
						String resp = in.readLine();
						System.out.println(testId + ":\tresp: " + resp);
						if (resp != null && !resp.equalsIgnoreCase("n/a")) {
								Node node = new Node();
								node.serverId = Long.parseLong(resp.split(":")[0]);
								node.address = resp.split(":")[1];
								node.portId = Short.parseShort(resp.split(":")[2]);
								in.close();
								return node;
						}
				} catch (IOException e) {
						throw new RuntimeException("Invalid response from the cache server!", e);
				} catch(NumberFormatException e){
                                                throw new RuntimeException("Invalid response from the cache server!", e);
                                }
				return null;
		}

		void trigger() {
				this.server.trigger();
		}
}
