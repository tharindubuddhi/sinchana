/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import sinchana.Server;
import sinchana.SinchanaInterface;
import sinchana.SinchanaTestInterface;
import sinchana.chord.FingerTableEntry;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 *
 * @author Hiru
 */
public class Tester implements SinchanaInterface, SinchanaTestInterface, Runnable {

		private Server server;
		private short testId;
		private ServerUI gui = null;
		private TesterController testerController;
		private Semaphore threadLock = new Semaphore(0);
		private Map<Long, Long> keySpace = new HashMap<Long, Long>();
		private boolean running = false;
		private long numOfTestingMessages = 0;

		/**
		 * 
		 * @param serverId
		 * @param anotherNode
		 * @param tc
		 */
		public Tester(short testId, TesterController tc) {
				try {
						this.testId = testId;
						this.testerController = tc;
						InetAddress[] ip = InetAddress.getAllByName("localhost");
//						System.out.println(testId + ": " + ip[0].toString());
//						server = new Server(ip[0],
//								(short) (testId + TesterController.LOCAL_PORT_ID_RANGE));
						server = new Server((short) (testId + TesterController.LOCAL_PORT_ID_RANGE));
						
                                                Node node = getRemoteNode(server.serverId,
								server.address, server.portId);
						server.setAnotherNode(node);
						server.registerSinchanaInterface(this);
						server.registerSinchanaTestInterface(this);
						server.startServer();
						if (TesterController.GUI_ON) {
								this.gui = new ServerUI(this);
						}
				} catch (UnknownHostException ex) {
						ex.printStackTrace();
				}
		}

		/**
		 * 
		 */
		public void startServer() {
				Thread thread = new Thread(this);
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
		public void startTest(long numOfTestingMessages) {
				this.numOfTestingMessages = numOfTestingMessages;
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

		@Override
		public Message receive(Message message) {
				Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_TESTER, 0,
						"Recieved " + message);
				Message response = null;
				switch (message.type) {
						case ACCEPT:
								Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 2,
										"Recieved ACCEPT message : " + message);
								break;
						case ERROR:
								Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 2,
										"Recieved ERROR message : " + message);
								break;
						case GET:
								Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 2,
										"Recieved GET message : " + message);
								requestCount++;
//								response = new Message(this.server, MessageType.ACCEPT, 1);
//								response.setTargetKey(message.getTargetKey());
								break;
				}
				return response;
		}

		@Override
		public void run() {
				try {
						if (this.gui != null) {
								this.gui.setServerId(server.serverId);
								this.gui.setVisible(true);
						}
						server.join();
						while (true) {
								threadLock.acquire();
								long randomDestination;
								while (numOfTestingMessages > 0) {
										randomDestination = (long) (Math.random() * Server.GRID_SIZE);
										server.send(randomDestination, "where are you?");
										numOfTestingMessages--;
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
						if (resp == null || resp.equalsIgnoreCase("null")) {
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 3,
										"Error in getting remote server details.");
								return null;
						}
						if (!resp.equalsIgnoreCase("n/a")) {
								Node node = new Node();
								node.serverId = Long.parseLong(resp.split(":")[0]);
								node.address = resp.split(":")[1];
								node.portId = Short.parseShort(resp.split(":")[2]);
								in.close();
								return node;
						}
				} catch (IOException e) {
						throw new RuntimeException("Invalid response from the cache server!", e);
				} catch (NumberFormatException e) {
						throw new RuntimeException("Invalid response from the cache server!", e);
				}
				return null;
		}

		void trigger() {
				this.server.trigger();
		}
		private long inputMessageCount = 0;
		private long avarageInputMessageQueueSize = 0;
		private long inputMessageQueueTimesCount = 0;
		private long avarageOutputMessageQueueSize = 0;
		private long outputMessageQueueTimesCount = 0;
		private long requestCount = 0;

		@Override
		public void incIncomingMessageCount() {
				inputMessageCount++;
		}

		@Override
		public void setMessageQueueSize(int size) {
				avarageInputMessageQueueSize += size;
				inputMessageQueueTimesCount++;
		}

		@Override
		public void setOutMessageQueueSize(int size) {
				avarageOutputMessageQueueSize += size;
				outputMessageQueueTimesCount++;
		}

		public long[] getTestData() {
				long[] data = new long[4];
				data[0] = inputMessageCount;
				data[1] = inputMessageQueueTimesCount == 0 ? 0 : (avarageInputMessageQueueSize / inputMessageQueueTimesCount);
				data[2] = outputMessageQueueTimesCount == 0 ? 0 : (avarageOutputMessageQueueSize / outputMessageQueueTimesCount);
				data[3] = requestCount;
				inputMessageCount = 0;
				avarageInputMessageQueueSize = 0;
				inputMessageQueueTimesCount = 0;
				avarageOutputMessageQueueSize = 0;
				outputMessageQueueTimesCount = 0;
				requestCount = 0;
				return data;
		}
}
