/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.math.BigInteger;
import java.util.Random;
import java.util.logging.Level;
import org.apache.thrift.TException;
import sinchana.SinchanaServer;
import sinchana.SinchanaRequestHandler;
import sinchana.SinchanaTestInterface;
import sinchana.util.logging.Logger;
import java.util.concurrent.Semaphore;
import sinchana.CONFIGURATIONS;
import sinchana.SinchanaResponseHandler;
import sinchana.util.tools.Hash;

/**
 *
 * @author Hiru
 */
public class Tester implements SinchanaTestInterface, Runnable {

	private SinchanaServer server;
	private int testId;
	private TesterController testerController;
	private Semaphore threadLock = new Semaphore(0);
	private long numOfTestingMessages = 0;
	String address = "127.0.0.1";
	String remoteNodeAddress = address + ":8000";

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
			server = new SinchanaServer(address + ":" + portId);
			server.registerSinchanaRequestHandler(new SinchanaRequestHandler() {

				@Override
				public byte[] request(byte[] message) {
//					TesterController.incTotalCount();
					inc();
					return RETURN_MESSAGE;
				}
			});
			server.registerSinchanaTestInterface(this);
			server.startServer();
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
	}

	/**
	 * 
	 */
	public void stopServer() {
		server.stopServer();
	}

	boolean isRunning() {
		return server.isRunning();
	}

	boolean isJoined() {
		return server.isJoined();
	}

	/**
	 * 
	 */
	public void startTest(long numOfTestingMessages) {
		this.numOfTestingMessages += numOfTestingMessages;
		threadLock.release();
	}

	/**
	 * 
	 */
	public void startRingTest() {
		this.server.testRing();
	}

	public void printTableInfo() {
		server.printTableInfo();
	}

	@Override
	public void run() {
		try {
			server.join(remoteNodeAddress);
			System.out.println(server.getServerIdAsString() + ": joined the ring");
			while (true) {
				threadLock.acquire();
				while (numOfTestingMessages > 0) {
					String val = new BigInteger(160, random).toString(CONFIGURATIONS.NUMBER_BASE);
					byte[] mid = Hash.generateId(val);
					server.sendRequest(mid, MESSAGE, null);
					numOfTestingMessages--;
				}
			}
		} catch (TException ex) {
			java.util.logging.Logger.getLogger(Tester.class.getName()).log(Level.SEVERE, null, ex);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
	}
	private SinchanaResponseHandler srh = new SinchanaResponseHandler() {

		@Override
		public void response(byte[] message) {
		}

		@Override
		public void error(byte[] message) {
			System.out.println(TAG_ERROR + new String(message));
		}
	};
	private static final byte[] MESSAGE = "Hi, Sinchana!".getBytes();
	private static final byte[] RETURN_MESSAGE = "Greetings :)".getBytes();
	private static final String TAG_ERROR = "ERROR: ";
	private final Random random = new Random();

	/**
	 * 
	 * @param isStable
	 */
	@Override
	public void setStable(boolean isStable) {
		if (isStable) {
			Logger.log(this.server.getNode(), Logger.LEVEL_INFO, Logger.CLASS_TESTER, 4,
					this.server.getServerIdAsString() + " is now stable!");
			testerController.incrementCompletedCount(this.testId);
		}
	}

	/**
	 * 
	 * @return
	 */
	public byte[] getServerId() {
		return server.getNode().getServerId();
	}

	public String getServerIdAsString() {
		return server.getServerIdAsString();
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

	@Override
	public boolean equals(Object obj) {
		return this.testId == ((Tester) obj).testId;
	}

	@Override
	public int hashCode() {
		return this.server.getNode().serverId.hashCode();
	}

	private synchronized void inc() {
		count++;
	}

	public int getCount() {
		int t = count;
		count = 0;
		return t;
	}
	private int count = 0;

	public long[] getTestData() {
		long[] data = new long[8];
		data[0] = inputMessageCount;
		data[1] = inputMessageQueueTimesCount == 0 ? 0 : (avarageInputMessageQueueSize / inputMessageQueueTimesCount);
		data[2] = maxInputMessageQueueSize;
		data[3] = requestCount;
		data[4] = requestViaPredecessorsCount;
		data[5] = requestLifetime;
		data[6] = (inputMessageQueueFull ? 1 : 0);
		data[7] = numOfTestingMessages;
		inputMessageCount = 0;
		avarageInputMessageQueueSize = 0;
		inputMessageQueueTimesCount = 0;
		tempMaxInputMessageQueueSize = 0;
		requestCount = 0;
		requestViaPredecessorsCount = 0;
		requestLifetime = 0;
		return data;
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
	public synchronized void incRequestCount(int lifetime, boolean routedViaPredecessors) {
		requestCount++;
		if (routedViaPredecessors) {
			requestViaPredecessorsCount++;
		}
		requestLifetime += lifetime;
	}

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
}
