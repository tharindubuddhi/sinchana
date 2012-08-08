/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.math.BigInteger;
import java.util.Set;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import sinchana.CONFIGURATIONS;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.util.tools.CommonTools;

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
	private final Timer timer2 = new Timer();
	private long numOfTestMessages = 0;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
//        Uncomment when you have a proxy network        
//        Properties props = System.getProperties();
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
					totalOutputMessageQueue, totalResolves,
					maxInputMessageQueueSize, maxOutputMessageQueueSize,
					totalLifeTime;
			long newTime, oldTime = Calendar.getInstance().getTimeInMillis();
			int mxaTester = -1;

			@Override
			public void run() {
				totalMessageIncome = 0;
				totalInputMessageQueue = 0;
				totalOutputMessageQueue = 0;
				maxInputMessageQueueSize = 0;
				maxOutputMessageQueueSize = 0;
				totalLifeTime = 0;
				totalResolves = 0;
				mxaTester = -1;
				long[] testData;
				Set<Integer> keySet = testServers.keySet();
				for (int tid : keySet) {
					testData = testServers.get(tid).getTestData();
					totalMessageIncome += testData[0];
					totalInputMessageQueue += testData[1];
					totalOutputMessageQueue += testData[2];
					if (maxInputMessageQueueSize < testData[3]) {
						maxInputMessageQueueSize = testData[3];
					}
					if (maxOutputMessageQueueSize < testData[4]) {
						maxOutputMessageQueueSize = testData[4];
						mxaTester = tid;
					}
					totalResolves += testData[5];
					totalLifeTime += testData[6];

				}
				newTime = Calendar.getInstance().getTimeInMillis();
				verifiedCount = 0;
				retrieveCount = 0;
				if (completedCount != 0) {
					cui.setStat("IC: " + (totalMessageIncome / completedCount)
							+ "    IB: " + (totalInputMessageQueue / completedCount)
							+ "    MI: " + maxInputMessageQueueSize
							+ "    OB: " + (totalOutputMessageQueue / completedCount)
							+ "    MO: " + maxOutputMessageQueueSize
							+ "    TR: " + totalResolves
							+ "    TP: " + (newTime > oldTime ? (totalResolves * 1000 / (newTime - oldTime)) : "INF") + "/S"
							+ "    AL: " + (totalResolves != 0 ? (totalLifeTime / totalResolves) : "NA")
							+ "    MS: " + dataMap.size()
							+ "    " + verifiedCount
							+ "    " + retrieveCount);
				}
				oldTime = newTime;
				if (mxaTester != -1) {
//										System.out.println(mxaTester + ": " + testServers.get(mxaTester).temp);
				}
			}
		}, 1000, 2000);
		timer2.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (numOfTestMessages != 0) {
					testMessages(numOfTestMessages / 10);
				}
			}
		}, 100, 100);
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
		String[] testServerIds = new String[NUM_OF_TESTING_NODES];

		for (int i = 0; i < NUM_OF_TESTING_NODES; i++) {
			testServerIds[i] = testServers.get(i).getServerId();
		}

		Arrays.sort(testServerIds);
		for (String id : testServerIds) {
			System.out.print(id + " ");
		}
		System.out.println("");
		Set<Integer> keySet = testServers.keySet();
		for (int key : keySet) {
			tester = testServers.get(key);
			if (!tester.isRunning()) {
				tester.startServer();
				System.out.println("Server " + tester.getServerId() + " is running...");
			}
		}
	}

	/**
	 * 
	 * @param numOfAutoTesters
	 */
	public void startAutoTest(long numOfTestMessages) {
		this.numOfTestMessages = numOfTestMessages;
	}
	private ConcurrentHashMap<String, TestData> dataMap = new ConcurrentHashMap<String, TestData>();
	private int verifiedCount = 0, retrieveCount = 0;
	private boolean verified = false;

	public void testMessages(long numOfTestMessages) {
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

	synchronized TestData getNextData() {
		verified = ((int) (Math.random() * dataMap.size())) % 2 != 0;
		if (verified) {
			while (true) {
				int rn = (int) (Math.random() * dataMap.size());
				Set<String> keySet = dataMap.keySet();
				for (String id : keySet) {
					if (rn-- == 0 && dataMap.get(id).verified && !dataMap.get(id).requested) {
						dataMap.get(id).requested = true;
						return dataMap.get(id);
					}
				}
			}
		} else {
			BigInteger bi = new BigInteger(160, new Random());
			while (dataMap.containsKey(bi.toString(16))) {
				bi = new BigInteger(160, new Random());
			}
			TestData td = new TestData();
			td.key = bi.toString(16);
			td.data = td.key;
			td.verified = false;
			td.requested = false;
			dataMap.put(td.key, td);
			return td;
		}
	}

	synchronized void setResponse(String targetKey) {
		if (dataMap.containsKey(targetKey)) {
			dataMap.get(targetKey).verified = true;
		} else {
			System.out.println("This is not acceptable!");
		}
	}

	synchronized void setResponse(String targetKey, String data) {
		if (!dataMap.remove(targetKey).data.equals(data)) {
			System.out.println("Failed :P");
		}
	}

	class TestData {

		String key;
		String data;
		boolean verified;
		boolean requested;
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
	public static int count = 0;

	public static synchronized void setMaxCount(int c) {
		if (c > count) {
			count = c;
			System.out.println("max: " + count);
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
				Message msg = new Message(testServers.get(key).getServer(), MessageType.REQUEST, 10);
				msg.setTargetKey(destination);
				msg.setMessage(text);
				testServers.get(key).getServer().send(msg);
			}
		}
	}
	String[] serviceArray = null;
	String[] keyArray = null;
	int serviceID = 0;

	public void publishService(int noOfServices) {

		serviceArray = new String[noOfServices];
		keyArray = new String[noOfServices];

		for (int i = 0; i < noOfServices; i++) {
			serviceArray[i] = String.valueOf("Service " + serviceID);
			keyArray[i] = CommonTools.generateId(serviceArray[i]).toString();
			serviceID++;
		}

		int randomId;
		int randomAmount;
		int x = 0;
		while (noOfServices > 0) {

			randomId = (int) (Math.random() * testServers.size());
			randomAmount = (int) (Math.random() * noOfServices);
			noOfServices = noOfServices - randomAmount;

			while (randomAmount > 0) {
				testServers.get(randomId).getServer().publishService(keyArray[x], serviceArray[x]);
				x++;
				randomAmount--;
			}
		}
	}

	public void retrieveService() {
		int randomId = 4;
		int randomAmount;
//		randomId = (int) (Math.random() * testServers.size());
        
		randomAmount = (int) (Math.random() * serviceArray.length);
		for (int i = 0; i < serviceArray.length; i++) {
			testServers.get(randomId).getServer().getService(keyArray[i]);
		}


	}
	String[] dataArray = null;
	String[] datakeyArray = null;
	int dataID = 1;

        
		public void storeData(int noOfData) {
                dataArray = new String[noOfData];
				datakeyArray = new String[noOfData];
                TestDataStore.storeCount = 0;
                TestDataStore.storeStartTime = System.currentTimeMillis();
				for (int i = 0; i < noOfData; i++) {
						dataArray[i] = String.valueOf("Data " + dataID);
						datakeyArray[i] = CommonTools.generateId(dataArray[i]).toString();
						dataID++;
				}
//                int randomId = (int) (Math.random() * testServers.size());
                int randomId = 4;
                
                for (int i = 0; i < noOfData; i++) {
                testServers.get(randomId).getServer().storeData(datakeyArray[i], dataArray[i]);
                
            }
   
		}
        
        
        
                public void retrieveData(){                  
                    TestDataStore.retrieveStartTime = System.currentTimeMillis();
                    int randomId = (int) (Math.random() * testServers.size());
                    for (int i = 0; i < datakeyArray.length; i++) {
                        testServers.get(randomId).getServer().getData(datakeyArray[i]);
                        
                    }

                }
                
                
                public void removeData(int randomAmount){
//                    int randomId = (int) (Math.random() * testServers.size());
                    TestDataStore.removeStartTime = System.currentTimeMillis();
                    int randomId = 4;
//                    int randomAmount = (int) (Math.random() * datakeyArray.length);
//                    System.out.println("random amount "+randomAmount);
                    for (int i = 0; i < randomAmount; i++) {
                        testServers.get(randomId).getServer().deleteData(datakeyArray[i]);                       
                    }
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

	public void trigger() {
		Set<Integer> keySet = testServers.keySet();
		for (int key : keySet) {
			testServers.get(key).trigger();
		}
	}
}
