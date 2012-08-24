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
import java.util.logging.Level;
import java.util.logging.Logger;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;

/**
 *
 * @author Hiru
 */
public class TesterController {

	public static final boolean GUI_ON = false;
	public static final boolean USE_REMOTE_CACHE_SERVER = false;
	public static final boolean CLEAR_CACHE_SERVER = true;
	public static final int ROUND_TRIP_TIME = 0;
	public static final boolean DO_LOG = false;
	public static final long WATCH_TIME_OUT = 10 * 1000; //milliseconds.
	private final Map<Integer, Tester> testServers = new HashMap<Integer, Tester>();
	private final ControllerUI cui = new ControllerUI(this);
	private final Timer timer = new Timer();

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
//        Uncomment when you have a proxy network        
		Properties props = System.getProperties();
//        props.put("http.proxyHost", "cache.mrt.ac.lk");
//        props.put("http.proxyPort", "3128");
		if (CLEAR_CACHE_SERVER) {
			LocalCacheServer.clear();
		}
		TesterController testerController = new TesterController();
	}

	private TesterController() {
		cui.setVisible(true);
		timer.scheduleAtFixedRate(new TimerTask() {

			long totalMessageIncome, totalInputMessageQueue,
					totalResolves, totalResolvesViaPredecessors,
					maxInputMessageQueueSize, totalLifeTime, numOfTestMsgs;
			long newTime, oldTime = System.currentTimeMillis();
			int numOfFullInputBuffs;
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
					cui.setStat(TAG_INCOMING_MSGS + (totalMessageIncome / completedCount)
							+ TAG_MAX_INCOMING_BUFFER_SIZE + maxInputMessageQueueSize
							+ TAG_TOTAL_RESOLVES + totalResolves
							+ TAG_ROUTED_VIA_PREDECESSORS + (totalResolves != 0 ? totalResolvesViaPredecessors * 100 / totalResolves : TAG_NOT_AVAILABLE)
							+ TAG_THROUGHPUT + (newTime > oldTime ? (totalResolves * 1000 / (newTime - oldTime)) : TAG_NOT_AVAILABLE) + "/S"
							+ TAG_AVARAGE_LIFE_TIME + (totalResolves != 0 ? (totalLifeTime / totalResolves) : TAG_NOT_AVAILABLE)
							+ TAG_NUM_OF_FULL_BUFFERS + numOfFullInputBuffs
							+ TAG_REMAINING_MSGS + numOfTestMsgs);
				}
				oldTime = newTime;
			}
		}, 1000, 1000);
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (numOfTestMsg > 0) {
					test(numOfTestMsg / 100);
				}
			}
		}, 1000, 10);
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
			if (!tester.isRunning()) {
				tester.startServer();
			}
		}
	}
	private int numOfTestingNodes = 0;
	private int completedCount = 0;
	private int numOfTestMsg = 0;

	/**
	 * 
	 * @param numOfAutoTesters
	 */
	public void startAutoTest(long numOfTestMessages) {
		numOfTestMsg = (int) numOfTestMessages;
	}

	public void test(long numOfTestMessages) {
		int numOfTestServers = testServers.size();
		int randomId;
		long randomAmount = 0;
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
		cui.setStatus(completedCount + " of " + numOfTestingNodes + " are stable...");
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
			dataArray[i] = DATA_TAG + dataID;
			datakeyArray[i] = KEY_TAG + dataID;
			dataID++;
		}
		dataHandlerobject.startStoreTime = System.currentTimeMillis();
		dataHandlerobject.storeSuccessCount = 0;
		dataHandlerobject.storeFailureCount = 0;
		for (int i = 0; i < dataArray.length; i++) {
			try {
				testServers.get(storeNodeID).getServer().storeData(datakeyArray[i].getBytes(), dataArray[i].getBytes(), dataHandlerobject);
			} catch (InterruptedException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}
	long startRetrieveTime = 0;
	private static final String DATA_TAG = "data ";
	private static final String KEY_TAG = "key ";

	public void retrieveData() {
		dataHandlerobject.startRetrieveTime = System.currentTimeMillis();
		dataHandlerobject.retrieveSuccessCount = 0;
		dataHandlerobject.retrieveFailureCount = 0;
		for (int i = 0; i < datakeyArray.length; i++) {
			try {
				testServers.get(retrieveNodeID).getServer().getData(datakeyArray[i].getBytes(), dataHandlerobject);
			} catch (InterruptedException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			}
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

	void resetAndWatch() {
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				long time = System.currentTimeMillis();
				System.out.println(time - prevTime + "ms\t\tcount = " + totalCount);
				totalCount = 0;
				prevTime = time;
			}
		}, 0, WATCH_TIME_OUT);

	}
	private long prevTime = System.currentTimeMillis();

	public static synchronized void incTotalCount() {
		totalCount++;
	}
	private static int totalCount = 0;

	public static synchronized void incErrorCount() {
		errorCount++;
	}
	private static int errorCount = 0;
}
