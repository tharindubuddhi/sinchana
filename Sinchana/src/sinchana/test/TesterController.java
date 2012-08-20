/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import sinchana.CONFIGURATIONS;
import sinchana.dataStore.SinchanaDataHandler;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class TesterController {

	public static int NUM_OF_TESTING_NODES = 0;
	public static int NUM_OF_AUTO_TESTING_NODES = 1;
	public static int max_buffer_size = 0;
	private final Map<Integer, Tester> testServers = new HashMap<Integer, Tester>();
	private final ControllerUI cui = new ControllerUI(this);
	private int completedCount = 0;
	private final Timer timer = new Timer();
	public volatile static long errorCount = 0;
	public volatile static long totalCount = 0;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
//        Uncomment when you have a proxy network        
		Properties props = System.getProperties();
//        props.put("http.proxyHost", "cache.mrt.ac.lk");
//        props.put("http.proxyPort", "3128");
		if (CONFIGURATIONS.CLEAR_CACHE_SERVER) {
			LocalCacheServer.clear();
		}
		TesterController testerController = new TesterController();
	}

	private TesterController() {
		cui.setVisible(true);
		timer.scheduleAtFixedRate(new TimerTask() {

			long totalMessageIncome, totalInputMessageQueue,
					totalResolves, totalResolvesViaPredecessors,
					maxInputMessageQueueSize, totalLifeTime;
			long newTime, oldTime = System.currentTimeMillis();
			int numOfFullInputBuffs;
			byte[] maxTester = "n/a".getBytes();

			@Override
			public void run() {
				totalMessageIncome = 0;
				totalInputMessageQueue = 0;
				maxInputMessageQueueSize = 0;
				totalLifeTime = 0;
				totalResolves = 0;
				totalResolvesViaPredecessors = 0;
				numOfFullInputBuffs = 0;
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
				}
				newTime = System.currentTimeMillis();
				if (completedCount != 0) {
					cui.setStat("IC: " + (totalMessageIncome / completedCount)
							+ "   MI: " + maxInputMessageQueueSize
							+ "   TR: " + totalResolves
							+ "   RP: " + (totalResolves != 0 ? totalResolvesViaPredecessors * 100 / totalResolves : "NA")
							+ "   TP: " + (newTime > oldTime ? (totalResolves * 1000 / (newTime - oldTime)) : "INF") + "/S"
							+ "   AL: " + (totalResolves != 0 ? (totalLifeTime / totalResolves) : "NA")
							+ "   FI: " + numOfFullInputBuffs);
				}
				oldTime = newTime;
//				if (!Arrays.equals(maxTester, maxTesterOld)) {
//					System.out.println("Max: " + ByteArrays.toReadableString(maxTester).toUpperCase());
//					maxTesterOld = maxTester;
//				}
			}
		}, 1000, 1000);
	}

	/**
	 * 
	 * @param numOfTesters
	 */
	public void startNodeSet(int portRange, int numOfTesters) {
		Tester tester;
		for (int i = NUM_OF_TESTING_NODES; i < NUM_OF_TESTING_NODES + numOfTesters; i++) {
			tester = new Tester(i, portRange + i, this);
			testServers.put(i, tester);
		}
		NUM_OF_TESTING_NODES += numOfTesters;
		byte[][] testServerIds = new byte[NUM_OF_TESTING_NODES][20];

		for (int i = 0; i < NUM_OF_TESTING_NODES; i++) {
			testServerIds[i] = testServers.get(i).getServerId();
		}

		for (byte[] id : testServerIds) {
			System.out.print(ByteArrays.toReadableString(id) + " ");
		}
		System.out.println("");
		Set<Integer> keySet = testServers.keySet();
		for (int key : keySet) {
			tester = testServers.get(key);
			if (!tester.isRunning()) {
				tester.startServer();
			}
		}
	}

	/**
	 * 
	 * @param numOfAutoTesters
	 */
	public void startAutoTest(long numOfTestMessages) {
		int numOfTestServers = testServers.size();
		int randomId;
		long randomAmount = 0;
		totalCount = 0;
		errorCount = 0;
		while (numOfTestMessages > 0) {
			randomId = (int) (Math.random() * numOfTestServers);
			if (numOfTestMessages > 10) {
				randomAmount = (long) (Math.random() * numOfTestMessages);
				numOfTestMessages -= randomAmount;
			} else {
				randomAmount = numOfTestMessages;
				numOfTestMessages = 0;
			}
			testServers.get(randomId).startTest(randomAmount);
		}
	}

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
	 * @param id
	 */
	public synchronized void incrementCompletedCount(int id) {
		completedCount++;
		cui.setStatus(completedCount + " of " + NUM_OF_TESTING_NODES + " are stable...");
	}

	/**
	 * 
	 * @param text
	 * @param destination
	 * @param requester
	 */
	public void send(String text, String destination, String requester) {
		Set<Integer> keySet = testServers.keySet();
		for (int key : keySet) {
			if (testServers.get(key).getServerId().equals(requester)) {
				Message msg = new Message(MessageType.REQUEST, testServers.get(key).getServer().getNode(), 10);
				msg.setDestinationId(destination.getBytes());
				msg.setData(text.getBytes());
//				testServers.get(key).getServer().send(msg);
			}
		}
	}
	String[] dataArray = null;
	String[] datakeyArray = null;
	int dataID = 1;
	int storeNodeID = 5, retrieveNodeID = 7;
	SinchanaDataHandlerImpl dataHandlerobject = new SinchanaDataHandlerImpl();

	public void storeData(int noOfData) {

		dataArray = new String[noOfData];
		datakeyArray = new String[noOfData];

		for (int i = 0; i < noOfData; i++) {
			dataArray[i] = "data " + dataID;
			datakeyArray[i] = "key " + dataID;
			dataID++;
		}
		dataHandlerobject.startStoreTime = System.currentTimeMillis();
		dataHandlerobject.storeSuccessCount = 0;
		dataHandlerobject.storeFailureCount = 0;
		for (int i = 0; i < dataArray.length; i++) {
			testServers.get(storeNodeID).getServer().storeData(datakeyArray[i].getBytes(), dataArray[i].getBytes(), dataHandlerobject);
		}
	}
	long startRetrieveTime = 0;

	public void retrieveData() {
		dataHandlerobject.startRetrieveTime = System.currentTimeMillis();
		dataHandlerobject.retrieveSuccessCount = 0;
		dataHandlerobject.retrieveFailureCount = 0;
		for (int i = 0; i < datakeyArray.length; i++) {
//            byte[] a = 
			testServers.get(retrieveNodeID).getServer().getData(datakeyArray[i].getBytes(), dataHandlerobject);
//            System.out.println(a);
		}
	}

	public void removeData(int randomAmount) {
	}

	/**
	 * 
	 * @param nodeIdsString
	 * @param typesString
	 * @param classIdsString
	 * @param locationsString
	 */
	public void printLogs(String nodeIdsString, String typesString, String classIdsString,
			String locationsString, String containTextString) {
		String[] temp;
		String[] nodeIds = null;
		int[] levels = null, classIds = null, locations = null;
		if (nodeIdsString.length() > 0) {
			nodeIds = nodeIdsString.split(" ");
		}
		if (typesString.length() > 0) {
			temp = typesString.split(" ");
			levels = new int[temp.length];
			for (int i = 0; i < temp.length; i++) {
				levels[i] = Integer.parseInt(temp[i]);
			}
		}
		if (classIdsString.length() > 0) {
			temp = classIdsString.split(" ");
			classIds = new int[temp.length];
			for (int i = 0; i < temp.length; i++) {
				classIds[i] = Integer.parseInt(temp[i]);
			}
		}
		if (locationsString.length() > 0) {
			temp = locationsString.split(" ");
			locations = new int[temp.length];
			for (int i = 0; i < temp.length; i++) {
				locations[i] = Integer.parseInt(temp[i]);
			}
		}
		sinchana.util.logging.Logger.print(nodeIds, levels, classIds, locations, containTextString);
	}
}
