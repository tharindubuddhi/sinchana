/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import sinchana.dataStore.SinchanaDataStoreImpl;
import sinchana.exceptions.SinchanaInvalidArgumentException;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.util.tools.Hash;

/**
 *
 * @author Hiru
 */
public class TesterController {

	public static final boolean USE_REMOTE_CACHE_SERVER = false;
	public static final boolean CLEAR_CACHE_SERVER = true;
	public static final int ROUND_TRIP_TIME = 0;
	public static final boolean DO_LOG = false;
	public static final long WATCH_TIME_OUT = 5 * 1000; //milliseconds.
	private final Map<Integer, Tester> testServers = new HashMap<Integer, Tester>();
	private final ControllerUI cui = new ControllerUI(this);
	private final Timer timer = new Timer();

	/**
	 * 
	 * @param args
	 */
    
	public static void main(String[] args) {
//        Uncomment when you have a proxy network and you need to connect to internet
        
//		Properties props = System.getProperties();
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
			if (!tester.isJoined()) {
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

	public void test(int numOfTestMessages) {
		int amount = numOfTestMessages / testServers.size();
		Collection<Tester> testers = testServers.values();
		for (Tester tester : testers) {
			tester.startTest(amount);
		}
	}

	public void testWithOneThread(int numOfMsgs) {
		this.totalCountAccumilated = 0;
		int numOfTestServers = testServers.size();
		while (numOfMsgs > 0) {
			String val = new BigInteger(160, random).toString(16);
			byte[] mid = Hash.generateId(val);
			int tid = (int) (Math.random() * numOfTestServers);
			try {
				testServers.get(tid).getServer().sendRequest(mid, MESSAGE, null);
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
        dataHandlerobject.FailureCount = 0;
//        SinchanaDataStoreImpl dataimpl = (SinchanaDataStoreImpl) testServers.get(storeNodeID).getServer().getSinchanaDataStoreInterface();  
//        SinchanaDataStoreImpl.storeSuccessCount = 0;
//        SinchanaDataStoreImpl.lastCount=0;
//        SinchanaDataStoreImpl.startStoreTime = System.currentTimeMillis();
//        SinchanaDataStoreImpl.lastTime = System.currentTimeMillis();
		for (int i = 0; i < dataArray.length; i++) {
			             
                try {
                    testServers.get(storeNodeID).getServer().storeData(datakeyArray[i].getBytes(), dataArray[i].getBytes(),dataHandlerobject);
                } catch (InterruptedException ex) {
                    Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
                } 
			
		}
	}
	String[] dataArray = null;
	String[] datakeyArray = null;
	int dataID = 1;
	int storeNodeID = 5, retrieveNodeID = 7;
	SinchanaDataHandlerImpl dataHandlerobject = new SinchanaDataHandlerImpl();
	long startRetrieveTime = 0;
	private static final String DATA_TAG = "data ";
	private static final String KEY_TAG = "key ";
    int retrieveTimeOut = 2000;
    
	public void retrieveData() {
		dataHandlerobject.startRetrieveTime = System.currentTimeMillis();
		dataHandlerobject.retrieveSuccessCount = 0;
		dataHandlerobject.FailureCount = 0;
		for (int i = 0; i < datakeyArray.length; i++) {
			try {
				testServers.get(retrieveNodeID).getServer().getData(datakeyArray[i].getBytes(), dataHandlerobject);


			} catch (InterruptedException ex) {
				Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}
    
    public void retrieveDataContinous() {
        dataHandlerobject.retrieveContinous = true;
       timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                System.out.println("retrieve count per 2 seconds: "+dataHandlerobject.retrieveSuccessCount);
                retrieveData();
            }
        }, 0, retrieveTimeOut);
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
				long tc = 0;
				Collection<Tester> testers = testServers.values();
				for (Tester tester : testers) {
					tc += tester.getCount();
//					System.out.println(tester.getServerIdAsString() + ": " + c);
				}
				totalCountAccumilated += tc;
				System.out.println(time - prevTime + "ms\t\tcount: " + tc
						+ "\t\tacumilated count: " + totalCountAccumilated + "\t\tthroughput: " + (tc * 1000 / (time - prevTime)));
				prevTime = time;
			}
		}, 0, WATCH_TIME_OUT);
	}
    
	private long prevTime = System.currentTimeMillis();
	private long totalCountAccumilated = 0;

	public static synchronized void incTotalCount() {
		totalCount++;
	}
	private static int totalCount = 0;

	public static synchronized void incErrorCount() {
		errorCount++;
	}
	private static int errorCount = 0;
}
