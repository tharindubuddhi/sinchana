/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Random;
import sinchana.SinchanaServer;
import sinchana.SinchanaRequestHandler;
import sinchana.SinchanaTestInterface;
import sinchana.chord.FingerTableEntry;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.concurrent.Semaphore;
import sinchana.CONFIGURATIONS;
import sinchana.SinchanaResponseHandler;

/**
 *
 * @author Hiru
 */
public class Tester implements SinchanaTestInterface, Runnable {

	private SinchanaServer server;
	private int testId;
	private ServerUI gui = null;
	private TesterController testerController;
	private Semaphore threadLock = new Semaphore(0);
	private boolean running = false;
	private long numOfTestingMessages = 0;

	/**
	 * 
	 * @param serverId
	 * @param anotherNode
	 * @param tc
	 */
	public Tester(int testId, int portId, TesterController tc) {
		try {
			this.testId = testId;
			this.testerController = tc;
			String address, remoteNodeAddress;
			address = InetAddress.getLocalHost().getHostAddress();
//			address = "127.0.0.1";
			remoteNodeAddress = LocalCacheServer.getRemoteNode(address, portId);
			remoteNodeAddress = address + ":8000";
			server = new SinchanaServer(address + ":" + portId, remoteNodeAddress);
			server.registerSinchanaRequestHandler(new SinchanaRequestHandler() {

				@Override
				public byte[] request(byte[] message) {
					return ("Hi " + new String(message) + ", Greetings from " + server.getServerIdAsString()).getBytes();
				}
			});
			server.registerSinchanaTestInterface(this);
			server.startServer();
			if (CONFIGURATIONS.GUI_ON) {
				this.gui = new ServerUI(this);
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
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
	private long startTime = -1, endTime = -1;

	public void startTest(long numOfTestingMessages) {
		this.numOfTestingMessages += numOfTestingMessages;
		startTime = System.currentTimeMillis();
		threadLock.release();
	}

	/**
	 * 
	 */
	public void startRingTest() {
		this.server.testRing();
	}
	private SinchanaResponseHandler srh = new SinchanaResponseHandler() {

		@Override
		public void response(byte[] message) {
			TesterController.successCount++;
			if ((TesterController.successCount+ TesterController.failureCount )% 100 == 0) {
				endTime = System.currentTimeMillis();
				System.out.println("Num of Messages " 
						+ TesterController.successCount + "/" + TesterController.failureCount 
						+ " @ " + (endTime - startTime) + "ms");
			}
		}

		@Override
		public void error(byte[] message) {
			TesterController.failureCount++;
			System.out.println("error : " + new String(message));
		}
	};
	Random random = new Random();

	@Override
	public void run() {
		try {
			if (this.gui != null) {
				this.gui.setServerId(new String(server.getNode().getServerId()));
				this.gui.setVisible(true);
			}
			if (server.join()) {
				System.out.println(server.getServerIdAsString() + ": joined the ring");
				while (true) {
					threadLock.acquire();
					while (numOfTestingMessages > 0) {
						BigInteger bi = new BigInteger(160, random);
						server.request(bi.toByteArray(), "Hiru".getBytes(), srh);
						numOfTestingMessages--;
					}
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
			Logger.log(this.server.getNode(), Logger.LEVEL_INFO, Logger.CLASS_TESTER, 4,
					this.server.getServerIdAsString() + " is now stable!");
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
			this.gui.setPredecessorId(predecessor != null ? new String(predecessor.getServerId()) : "n/a");
		}
	}

	/**
	 * 
	 * @param successor
	 */
	@Override
	public void setSuccessor(Node successor) {
		if (this.gui != null) {
			this.gui.setSuccessorId(successor != null ? new String(successor.getServerId()) : "n/a");
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
	public byte[] getServerId() {
		return server.getNode().getServerId();
	}

	public int getTestId() {
		return testId;
	}

	/**
	 * 
	 * @return
	 */
	public SinchanaServer getServer() {
		return server;
	}

	public boolean isRunning() {
		return running;
	}

	public ServerUI getGui() {
		return gui;
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

	public void send(String dest, String msg) {
		this.server.request(dest.getBytes(), msg.getBytes());
	}

	@Override
	public boolean equals(Object obj) {
		return this.testId == ((Tester) obj).testId;
	}

	@Override
	public int hashCode() {
		return this.server.getNode().serverId.hashCode();
	}
	private long inputMessageCount = 0;
	private long avarageInputMessageQueueSize = 0;
	private long maxInputMessageQueueSize = 0;
	private long tempMaxInputMessageQueueSize = 0;
	private long inputMessageQueueTimesCount = 0;
	private long requestCount = 0;
	private long requestViaPredecessorsCount = 0;
	private long requestLifetime = 0;
	private boolean inputMessageQueueFull = false;

	@Override
	public synchronized void incIncomingMessageCount() {
		inputMessageCount++;
	}

	@Override
	public synchronized void setMessageQueueSize(int size) {
		tempMaxInputMessageQueueSize = Math.max(size, tempMaxInputMessageQueueSize);
		maxInputMessageQueueSize = tempMaxInputMessageQueueSize;
		inputMessageQueueFull = size >= CONFIGURATIONS.INPUT_MESSAGE_BUFFER_SIZE - 1;
		avarageInputMessageQueueSize += size;
		inputMessageQueueTimesCount++;
	}

	public long[] getTestData() {
		long[] data = new long[10];
		data[0] = inputMessageCount;
		data[1] = inputMessageQueueTimesCount == 0 ? 0 : (avarageInputMessageQueueSize / inputMessageQueueTimesCount);
		data[2] = maxInputMessageQueueSize;
		data[3] = requestCount;
		data[4] = requestViaPredecessorsCount;
		data[5] = requestLifetime;
		data[6] = (inputMessageQueueFull ? 1 : 0);
		inputMessageCount = 0;
		avarageInputMessageQueueSize = 0;
		inputMessageQueueTimesCount = 0;
		tempMaxInputMessageQueueSize = 0;
		requestCount = 0;
		requestViaPredecessorsCount = 0;
		requestLifetime = 0;
		return data;
	}

	@Override
	public synchronized void incRequestCount(int lifetime, boolean routedViaPredecessors) {
		requestCount++;
		if (routedViaPredecessors) {
			requestViaPredecessorsCount++;
		}
		requestLifetime += lifetime;
	}
}
