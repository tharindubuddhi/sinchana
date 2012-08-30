/************************************************************************************

 * Sinchana Distributed Hash table 

 * Copyright (C) 2012 Sinchana DHT - Department of Computer Science &               
 * Engineering, University of Moratuwa, Sri Lanka. Permission is hereby 
 * granted, free of charge, to any person obtaining a copy of this 
 * software and associated documentation files of Sinchana DHT, to deal 
 * in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.

 * Neither the name of University of Moratuwa, Department of Computer Science 
 * & Engineering nor the names of its contributors may be used to endorse or 
 * promote products derived from this software without specific prior written 
 * permission.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
 * SOFTWARE.                                                                    
 ************************************************************************************/
package sinchana.test;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import sinchana.SinchanaResponseCallback;
import sinchana.dataStore.SinchanaDataCallback;
import sinchana.exceptions.SinchanaInvalidArgumentException;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.service.SinchanaServiceCallback;
import sinchana.test.examples.service.HelloService;
import sinchana.util.tools.Hash;

/**
 *This class creates given number of testers and controls them.
 * @author Hirantha Subasinghe
 */
public class TesterController {

	/**
	 * The round trip time in milliseconds. This will induce an arbitry round trip time
	 * to the system. Keeping is to zero will have no effect.
	 */
	public static final int ROUND_TRIP_TIME = 0;
	/**
	 * <code>true</code> if the logging should be done. <code>false</code> otherwise.
	 */
	public static final boolean DO_LOG = false;
	private final Map<Integer, Tester> testServers = new HashMap<Integer, Tester>();
	private final ControllerUI cui = new ControllerUI(this);
	private final Timer timer = new Timer();

	public static void main(String[] args) {
//        Uncomment when you have a proxy network and you need to connect to internet

//		Properties props = System.getProperties();
//      props.put("http.proxyHost", "cache.mrt.ac.lk");
//      props.put("http.proxyPort", "3128");
		TesterController testerController = new TesterController();
	}

	private TesterController() {
		cui.setVisible(true);
		timer.scheduleAtFixedRate(new TimerTask() {

			long totalMessageIncome, totalInputMessageQueue,
					totalResolves, totalResolvesViaPredecessors,
					maxInputMessageQueueSize, totalLifeTime, numOfTestMsgs;
			long newTime, oldTime = System.currentTimeMillis();
			int numOfFullInputBuffs, timerCount = 0, timeOut = 10;
			byte[] maxTester = TAG_NOT_AVAILABLE.getBytes();

			@Override
			public void run() {
				totalMessageIncome = 0;
				totalInputMessageQueue = 0;
				maxInputMessageQueueSize = 0;
				totalLifeTime = 0;
				totalResolves = 0;
				totalResolvesViaPredecessors = 0;
				numOfFullInputBuffs = 0;
				numOfTestMsgs = 0;
				long[] testData;
				Set<Integer> keySet = testServers.keySet();
				for (int tid : keySet) {
					testData = testServers.get(tid).getTestData();
					totalMessageIncome += testData[0];
					totalInputMessageQueue += testData[1];
					if (maxInputMessageQueueSize < testData[2]) {
						maxInputMessageQueueSize = testData[2];
						maxTester = testServers.get(tid).getServerId();
					}
					totalResolves += testData[3];
					totalResolvesViaPredecessors += testData[4];
					totalLifeTime += testData[5];
					numOfFullInputBuffs += testData[6];
					numOfTestMsgs += testData[7];
				}
				newTime = System.currentTimeMillis();
				if (completedCount != 0) {
					cui.setStat(TAG_INCOMING_MSGS + totalMessageIncome
							+ TAG_MAX_INCOMING_BUFFER_SIZE + maxInputMessageQueueSize
							+ TAG_TOTAL_RESOLVES + totalResolves
							+ TAG_ROUTED_VIA_PREDECESSORS + (totalResolves != 0 ? totalResolvesViaPredecessors * 100 / totalResolves : TAG_NOT_AVAILABLE)
							+ TAG_THROUGHPUT + (newTime > oldTime ? (totalResolves * 1000 / (newTime - oldTime)) : TAG_NOT_AVAILABLE) + "/S"
							+ TAG_AVARAGE_LIFE_TIME + (totalResolves != 0 ? (totalLifeTime / totalResolves) : TAG_NOT_AVAILABLE)
							+ TAG_NUM_OF_FULL_BUFFERS + numOfFullInputBuffs
							+ TAG_REMAINING_MSGS + numOfTestMsgs);
				}
				if (++timerCount >= timeOut) {
					timerCount = 0;
					System.out.println("In last " + timeOut + " Seconds\tTotal resolves: " + totalCount + "\tErrors: " + errorCount);
					totalCount = 0;
					errorCount = 0;
				}
				oldTime = newTime;
			}
		}, 1000, 1000);
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (sendContinuously && numOfRequest > 0) {
					switch (testCaseSelection) {
						case MESSAGE_TESTING:
							testMessages(numOfRequest / 10);
							break;
						case DATA_STORE_TESTING:
							getData(numOfRequest / 10);
							break;
						case SERVICE_TESTING:
							invokeServiceMethod(numOfRequest / 10);
							break;
					}

				}
			}
		}, 1000, 100);
	}
	private static final String TAG_INCOMING_MSGS = "   IC: ";
	private static final String TAG_MAX_INCOMING_BUFFER_SIZE = "   MI: ";
	private static final String TAG_TOTAL_RESOLVES = "   TR: ";
	private static final String TAG_ROUTED_VIA_PREDECESSORS = "   RP: ";
	private static final String TAG_THROUGHPUT = "   TP: ";
	private static final String TAG_AVARAGE_LIFE_TIME = "   AL: ";
	private static final String TAG_NUM_OF_FULL_BUFFERS = "   FB: ";
	private static final String TAG_REMAINING_MSGS = "   RM: ";
	private static final String TAG_NOT_AVAILABLE = "n/a";

	/**
	 * 
	 * @param portRange 
	 * @param numOfTesters
	 */
	public void startNodeSet(int portRange, int numOfTesters) {
		Tester tester;
		for (int i = numOfTestingNodes; i < numOfTestingNodes + numOfTesters; i++) {
			tester = new Tester(i, portRange + i, this);
			testServers.put(i, tester);
		}
		numOfTestingNodes += numOfTesters;
		byte[][] testServerIds = new byte[numOfTestingNodes][20];

		for (int i = 0; i < numOfTestingNodes; i++) {
			testServerIds[i] = testServers.get(i).getServerId();
		}

		Set<Integer> keySet = testServers.keySet();
		for (int key : keySet) {
			tester = testServers.get(key);
			if (!tester.isJoined()) {
				tester.startServer();
			}
		}
	}
	private int numOfTestingNodes = 0;
	private int completedCount = 0;

	/**
	 * 
	 */
	public void startRingTest() {
		Set<Integer> keySet = testServers.keySet();
		for (int key : keySet) {
			testServers.get(key).startRingTest();
			break;
		}
	}

	/**
	 * 
	 * @param idText
	 */
	public void printTableInfo(String idText) {
		BigInteger id = new BigInteger(idText, 16);
		Collection<Tester> testers = testServers.values();
		for (Tester tester : testers) {
			if (id.compareTo(new BigInteger(1, tester.getServerId())) == 0) {
				tester.printTableInfo();
				return;
			}
		}
		System.out.println("No such id found: " + idText);
	}

	/**
	 * 
	 * @param id
	 */
	public synchronized void incrementCompletedCount(int id) {
		completedCount++;
		cui.setStatus(completedCount + " of " + numOfTestingNodes + " are stable...");
	}

	/**
	 * 
	 * @param nodeIdsString
	 * @param typesString
	 * @param classIdsString
	 * @param locationsString
	 * @param containTextString  
	 */
	public void printLogs(String nodeIdsString, String typesString, String classIdsString,
			String locationsString, String containTextString) {
		String[] temp;
		String[] nodeIds = null;
		int[] levels = null, classIds = null, locations = null;
		if (nodeIdsString.length() > 0) {
			nodeIds = nodeIdsString.split(FILTER_SPLITTER);
		}
		if (typesString.length() > 0) {
			temp = typesString.split(FILTER_SPLITTER);
			levels = new int[temp.length];
			for (int i = 0; i < temp.length; i++) {
				levels[i] = Integer.parseInt(temp[i]);
			}
		}
		if (classIdsString.length() > 0) {
			temp = classIdsString.split(FILTER_SPLITTER);
			classIds = new int[temp.length];
			for (int i = 0; i < temp.length; i++) {
				classIds[i] = Integer.parseInt(temp[i]);
			}
		}
		if (locationsString.length() > 0) {
			temp = locationsString.split(FILTER_SPLITTER);
			locations = new int[temp.length];
			for (int i = 0; i < temp.length; i++) {
				locations[i] = Integer.parseInt(temp[i]);
			}
		}
		sinchana.util.logging.Logger.print(nodeIds, levels, classIds, locations, containTextString);
	}
	private static final String FILTER_SPLITTER = " ";

	void setSelection(int selection) {
		numOfRequest = 0;
		sendContinuously = false;
		testCaseSelection = selection;
	}
	/**
	 * Test case messages 
	 */
	public static final int MESSAGE_TESTING = 1;
	/**
	 * Test case data store
	 */
	public static final int DATA_STORE_TESTING = 2;
	/**
	 * Test case services
	 */
	public static final int SERVICE_TESTING = 3;
	/**
	 * Current test case
	 */
	public int testCaseSelection = 1;

	void publishService() {
		try {
			testServers.get(0).getServer().publishService(serviceKey, helloService);
		} catch (InterruptedException ex) {
			Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SinchanaInvalidArgumentException ex) {
			Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	private byte[] serviceKey = "HelloService".getBytes();
	private byte[] serviceData = "Sinchana".getBytes();
	private HelloService helloService = new HelloService();

	void removeService() {
		try {
			testServers.get(0).getServer().removeService(serviceKey);
		} catch (InterruptedException ex) {
			Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SinchanaInvalidArgumentException ex) {
			Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	void invokeServiceMethod(int numOfInvoke) {
		try {
			int numOfTestServers = testServers.size();
			byte[] reference = testServers.get(0).getServer().discoverService(serviceKey);
			if (reference == null) {
				System.out.println(new String(serviceKey) + " is not found");
				return;
			}
			while (numOfInvoke > 0) {
				int tid = (int) (Math.random() * numOfTestServers);
				testServers.get(tid).getServer().invokeService(reference, serviceData, ssc);
				numOfInvoke--;
			}
		} catch (InterruptedException ex) {
			Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SinchanaTimeOutException ex) {
			Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	private SinchanaServiceCallback ssc = new SinchanaServiceCallback() {

		@Override
		public void serviceFound(byte[] key, boolean success, byte[] data) {
//			System.out.println("");
		}

		@Override
		public void serviceResponse(byte[] key, boolean success, byte[] data) {
			totalCount++;
		}

		@Override
		public void error(byte[] error) {
			totalCount++;
			errorCount++;
		}
	};

	void sendMsgs(int numOfMsgs) {
		this.sendContinuously = false;
		this.numOfRequest = 0;
		totalCount = 0;
		errorCount = 0;
		testMessages(numOfMsgs);
	}

	void sendMsgsContinuously(int numOfMsgs) {
		this.sendContinuously = true;
		this.numOfRequest = numOfMsgs;
		totalCount = 0;
		errorCount = 0;
	}

	void retrieveData(int numOfMsgs) {
		this.sendContinuously = false;
		this.numOfRequest = numOfMsgs;
		totalCount = 0;
		errorCount = 0;
		getData(numOfMsgs);
	}

	void retrieveDataContinuously(int numOfMsgs) {
		this.sendContinuously = true;
		this.numOfRequest = numOfMsgs;
		totalCount = 0;
		errorCount = 0;
	}

	void invokeService(int numOfMsgs) {
		this.sendContinuously = false;
		this.numOfRequest = numOfMsgs;
		totalCount = 0;
		errorCount = 0;
		invokeServiceMethod(numOfMsgs);
	}

	void invokeServiceContinuously(int numOfMsgs) {
		this.sendContinuously = true;
		this.numOfRequest = numOfMsgs;
		totalCount = 0;
		errorCount = 0;
	}
	private boolean sendContinuously = false;
	private int numOfRequest;
	private static int totalCount = 0;
	private static int errorCount = 0;

	public void testMessages(int numOfMsgs) {
		int numOfTestServers = testServers.size();
		while (numOfMsgs > 0) {
			String val = new BigInteger(160, random).toString(16);
			byte[] mid = Hash.generateId(val);
			int tid = (int) (Math.random() * numOfTestServers);
			try {
				testServers.get(tid).getServer().sendRequest(mid, MESSAGE, src);
			} catch (InterruptedException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			} catch (SinchanaInvalidArgumentException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			}
			numOfMsgs--;
		}
	}
	private final Random random = new Random();
	private final byte[] MESSAGE = "Hi, Sinchana".getBytes();
	private SinchanaResponseCallback src = new SinchanaResponseCallback() {

		@Override
		public synchronized void response(byte[] message) {
			totalCount++;
		}

		@Override
		public synchronized void error(byte[] error) {
			totalCount++;
			errorCount++;
		}
	};

	/**
	 * the test method to store a no of data from a randomly picked node
	 * @param noOfData No of data to be stored
	 */
	public void storeData(int noOfData) {
		totalCount = 0;
		errorCount = 0;
		dataArray = new String[noOfData];
		datakeyArray = new String[noOfData];
		int numOfTestServers = testServers.size();

		dataID = 0;
		for (int i = 0; i < noOfData; i++) {
			dataArray[i] = DATA_TAG + dataID;
			datakeyArray[i] = KEY_TAG + dataID;
			dataID++;
		}
		for (int i = 0; i < dataArray.length; i++) {
			try {
				int tid = (int) (Math.random() * numOfTestServers);
				testServers.get(tid).getServer().storeData(datakeyArray[i].getBytes(), dataArray[i].getBytes(), sdc);
			} catch (InterruptedException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			}

		}
	}

	/**
	 * the test method to remove no of data 
	 * @param numOfDataToRemove no of data to be removed
	 */
	public void removeData(int numOfDataToRemove) {
		totalCount = 0;
		errorCount = 0;
		if (dataArray == null || datakeyArray == null) {
			return;
		}
		int numOfTotalData = dataArray.length;
		int numOfTestServers = testServers.size();
		while (numOfDataToRemove > 0) {
			int randomDataId = (int) (Math.random() * numOfTotalData);
			int tid = (int) (Math.random() * numOfTestServers);
			try {
				testServers.get(tid).getServer().deleteData(datakeyArray[randomDataId].getBytes(), sdc);
			} catch (InterruptedException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			}
			numOfDataToRemove--;
		}
	}

	/**
	 * the method retrieves no of data from the sinchana dht
	 * @param numOfDataToRetrieve
	 */
	public void getData(int numOfDataToRetrieve) {
		if (dataArray == null || datakeyArray == null) {
			return;
		}
		int numOfTotalData = dataArray.length;
		int numOfTestServers = testServers.size();
		while (numOfDataToRetrieve > 0) {
			int randomDataId = (int) (Math.random() * numOfTotalData);
			int tid = (int) (Math.random() * numOfTestServers);
			try {
				testServers.get(tid).getServer().getData(datakeyArray[randomDataId].getBytes(), sdc);
			} catch (InterruptedException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			}
			numOfDataToRetrieve--;
		}
	}
	private String[] dataArray = null;
	private String[] datakeyArray = null;
	private int dataID = 0;
	private static final String DATA_TAG = "data ";
	private static final String KEY_TAG = "key ";
	private SinchanaDataCallback sdc = new SinchanaDataCallback() {

		@Override
		public synchronized void isStored(byte[] key, boolean success) {
			totalCount++;
		}

		@Override
		public synchronized void isRemoved(byte[] key, boolean success) {
			totalCount++;
		}

		@Override
		public synchronized void response(byte[] key, byte[] data) {
			totalCount++;
		}

		@Override
		public synchronized void error(byte[] error) {
			totalCount++;
			errorCount++;
		}
	};
}
