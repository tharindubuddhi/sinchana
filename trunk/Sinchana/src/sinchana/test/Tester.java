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
import sinchana.dataStore.SinchanaDataStoreImpl;
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
			address = "127.0.0.1";
			remoteNodeAddress = LocalCacheServer.getRemoteNode(address, portId);
			remoteNodeAddress = address + ":8000";
			server = new SinchanaServer(address + ":" + portId, remoteNodeAddress);
			SinchanaDataStoreImpl sdsi = new SinchanaDataStoreImpl();
			server.registerSinchanaStoreInterface(sdsi);
			server.registerSinchanaRequestHandler(new SinchanaRequestHandler() {

				@Override
				public synchronized byte[] request(byte[] message) {
					endTime = System.currentTimeMillis();
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
		public synchronized void response(byte[] message) {
			endTime = System.currentTimeMillis();
			long c = TesterController.incCount();
			System.out.println(server.getServerIdAsString() + ": " 
					+ new String(message) + "\t\t" + c + "\t" + (endTime - startTime));
		}

		@Override
		public void error(byte[] message) {
			throw new UnsupportedOperationException("Not supported yet.");
		}
	};
	Random random = new Random();

	@Override
	public void run() {
		try {
			if (this.gui != null) {
				this.gui.setServerId(new String(server.getServerId()));
				this.gui.setVisible(true);
			}
			server.join();
			System.out.println(server.getServerIdAsString() + ": joined the ring");
			while (true) {
				threadLock.acquire();
				while (numOfTestingMessages > 0) {
					BigInteger bi = new BigInteger(160, random);
					server.request(bi.toByteArray(), "Hiru".getBytes(), srh);
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
			Logger.log(this.server, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 4,
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
		return server.getServerId();
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
		return this.server.serverId.hashCode();
	}
	private long inputMessageCount = 0;
	private long avarageInputMessageQueueSize = 0;
	private long maxInputMessageQueueSize = 0;
	private long inputMessageQueueTimesCount = 0;
	private long avarageOutputMessageQueueSize = 0;
	private long maxOutputMessageQueueSize = 0;
	private long outputMessageQueueTimesCount = 0;
	private long requestCount = 0;
	private long requestLifetime = 0;

	@Override
	public synchronized void incIncomingMessageCount() {
		inputMessageCount++;
	}

	@Override
	public synchronized void setMessageQueueSize(int size) {
		if (maxInputMessageQueueSize < size) {
			maxInputMessageQueueSize = size;
		}
		avarageInputMessageQueueSize += size;
		inputMessageQueueTimesCount++;
	}

	@Override
	public synchronized void setOutMessageQueueSize(int size) {
		if (maxOutputMessageQueueSize < size) {
			maxOutputMessageQueueSize = size;
		}
		avarageOutputMessageQueueSize += size;
		outputMessageQueueTimesCount++;
	}

	public long[] getTestData() {
		long[] data = new long[7];
		data[0] = inputMessageCount;
		data[1] = inputMessageQueueTimesCount == 0 ? 0 : (avarageInputMessageQueueSize / inputMessageQueueTimesCount);
		data[2] = outputMessageQueueTimesCount == 0 ? 0 : (avarageOutputMessageQueueSize / outputMessageQueueTimesCount);
		data[3] = maxInputMessageQueueSize;
		data[4] = maxOutputMessageQueueSize;
		data[5] = requestCount;
		data[6] = requestLifetime;
		inputMessageCount = 0;
		avarageInputMessageQueueSize = 0;
		inputMessageQueueTimesCount = 0;
		avarageOutputMessageQueueSize = 0;
		outputMessageQueueTimesCount = 0;
		maxInputMessageQueueSize = 0;
		maxOutputMessageQueueSize = 0;
		requestCount = 0;
		requestLifetime = 0;
		return data;
	}
}
