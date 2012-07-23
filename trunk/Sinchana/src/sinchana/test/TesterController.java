/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Set;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import sinchana.Server;

/**
 *
 * @author Hiru
 */
public class TesterController {

		/**
		 * 
		 */
//		public static final String LOCAL_SERVER_ADDRESS = "localhost";
//		public static final String LOCAL_SERVER_ADDRESS = "10.8.108.59";
		/**
		 * 
		 */
		public static final short LOCAL_PORT_ID_RANGE = 8000;
		/**
		 * 
		 */
//		public static final String REMOTE_SERVER_ADDRESS = "localhost";
		/**
		 * 
		 */
//		public static final int REMOTE_SERVER_ID = 0;
		/**
		 * 
		 */
//		public static final short REMOTE_SERVER_PORT_ID = 8000 + REMOTE_SERVER_ID;
		/**
		 * 
		 */
		public static short NUM_OF_TESTING_NODES = 0;
		/**
		 * 
		 */
		public static short NUM_OF_AUTO_TESTING_NODES = 1;
		/**
		 * 
		 */
		public static final boolean GUI_ON = false;
		/**
		 * 
		 */
		public static final int AUTO_TEST_TIMEOUT = 2;
		public static final int ROUND_TIP_TIME = 0;
		public static final int AUTO_TEST_MESSAGE_LIFE_TIME = 120;
		/**
		 * 
		 */
		public static int max_buffer_size = 0;
		private Map<Short, Tester> testServers = new HashMap<>();
		private ControllerUI cui;
		private int completedCount = 0;
		private Semaphore startLock = new Semaphore(0);
		private Map<Long, Long> expectedCountMap = new HashMap<>();
		private Map<Long, Long> keySpace = new HashMap<>();

		/**
		 * 
		 * @param args
		 */
		public static void main(String[] args) {
				try {
						URL yahoo = new URL("http://cseanremo.appspot.com/remoteip?clear=true");
						URLConnection yc = yahoo.openConnection();
						InputStreamReader isr = new InputStreamReader(yc.getInputStream());
						isr.close();
				} catch (Exception e) {
						System.out.println("Error " + e.getLocalizedMessage());
				}
				TesterController testerController = new TesterController();
		}

		private TesterController() {
				cui = new ControllerUI(this);
				cui.setVisible(true);
		}

		/**
		 * 
		 * @param numOfTesters
		 */
		public void startNodeSet(short numOfTesters) {
				String[] coloms = {"Node ID", "Start", "End", "Duration"};
				cui.initTableInfo(coloms);
				try {
						Tester tester;
						for (short i = NUM_OF_TESTING_NODES; i < NUM_OF_TESTING_NODES + numOfTesters; i++) {
								tester = new Tester(i, this);
								testServers.put(i, tester);
						}
						NUM_OF_TESTING_NODES += numOfTesters;
						long[] testServerIds = new long[NUM_OF_TESTING_NODES];

						for (short i = 0; i < NUM_OF_TESTING_NODES; i++) {
								testServerIds[i] = testServers.get(i).getServerId();
						}

						Arrays.sort(testServerIds);
						for (long i : testServerIds) {
								System.out.print(i + " ");
						}
						System.out.println("");
//						generateKeySpace(testServerIds, keySpace);
//						generateExpectedCountMap(testServerIds);
						Set<Short> keySet = testServers.keySet();
						for (Short key : keySet) {
								Tester server = testServers.get(key);
								if (!server.isRunning()) {
										server.resetTester();
										server.startServer();
										System.out.println("Server " + server.getServerId() + " is running...");
								}
						}

						startLock.acquire();

						Object[][] tableData = new Object[NUM_OF_TESTING_NODES][4];
//						for (int i = 0; i < testServers.length; i++) {
//								tableData[i][0] = testServers[i].getServerId();
//								tableData[i][1] = testServers[i].getStartTime().get(Calendar.HOUR_OF_DAY)
//										+ ":" + testServers[i].getStartTime().get(Calendar.MINUTE)
//										+ ":" + testServers[i].getStartTime().get(Calendar.SECOND)
//										+ ":" + testServers[i].getStartTime().get(Calendar.MILLISECOND);
//								tableData[i][2] = testServers[i].getEndTime().get(Calendar.HOUR_OF_DAY)
//										+ ":" + testServers[i].getEndTime().get(Calendar.MINUTE)
//										+ ":" + testServers[i].getEndTime().get(Calendar.SECOND)
//										+ ":" + testServers[i].getEndTime().get(Calendar.MILLISECOND);
//								tableData[i][3] = testServers[i].getEndTime().getTimeInMillis()
//										- testServers[i].getStartTime().getTimeInMillis();
//						}
//						cui.setTableInfo(tableData);
				} catch (InterruptedException ex) {
						Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
				}
		}

		/**
		 * 
		 * @param numOfAutoTesters
		 */
		public void startAutoTest(short numOfAutoTesters) {
				String[] coloms = {"Node ID", "Expected", "Recieved", "Precentage", "Resolved", "Precentage"};
				cui.initTableInfo(coloms);

				NUM_OF_AUTO_TESTING_NODES = numOfAutoTesters;
				long[] testServerIds = new long[NUM_OF_TESTING_NODES];
				for (int i = 0; i < NUM_OF_TESTING_NODES; i++) {
//						testServerIds[i] = testServers[i].getServerId();
				}
				Arrays.sort(testServerIds);
				generateKeySpace(testServerIds, keySpace);
				generateExpectedCountMap(testServerIds);

				for (int i = 0; i < NUM_OF_TESTING_NODES; i++) {
//						testServers[i].setExpectedCount(expectedCountMap.get(testServers[i].getServerId()));
//						testServers[i].setRealKeySpace(keySpace);
//						testServers[i].resetTester();
				}
				Calendar startTime = Calendar.getInstance();
				for (int i = 0; i < NUM_OF_AUTO_TESTING_NODES; i++) {
//						testServers[i].startTest();
				}
				int tCount = 0;
				boolean running = true;
				long timeNow;
				while (running) {
						setAutoTestTableInfo();
						try {
								Thread.sleep(1000);
						} catch (InterruptedException ex) {
								Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
						}
						running = false;
						timeNow = Calendar.getInstance().getTimeInMillis();
						tCount++;
						cui.setStatus("   " + tCount);
//						for (Tester tester : testServers) {
//								if (timeNow - tester.getEndTime().getTimeInMillis() < AUTO_TEST_TIMEOUT * 1000) {
//										running = true;
//										break;
//								}
//						}
				}
				Calendar endTime = Calendar.getInstance();
				setAutoTestTableInfo();
				testKeySpaces();


				int resolvedCount = 0;
				int avarageLifeCount = 0;
//				for (Tester tester : testServers) {
//						resolvedCount += tester.getResolvedCount();
//						avarageLifeCount += tester.getLifeTimeCount();
//				}

				int averageLife = (int) (avarageLifeCount
						/ (TesterController.NUM_OF_AUTO_TESTING_NODES * Server.GRID_SIZE));
				System.out.println("Average life used is "
						+ (TesterController.AUTO_TEST_MESSAGE_LIFE_TIME - averageLife));
				long time = endTime.getTimeInMillis() - startTime.getTimeInMillis();
				long throughPut = resolvedCount * 1000 / time;
				System.out.println(resolvedCount + " resolves in " + time + "ms. Message throughput: " + throughPut);

		}

		private void setAutoTestTableInfo() {
				Object[][] tableData = new Object[NUM_OF_TESTING_NODES][6];
//				for (int i = 0; i < testServers.length; i++) {
//						tableData[i][0] = testServers[i].getServerId();
//						tableData[i][1] = expectedCountMap.get(testServers[i].getServerId());
//						tableData[i][2] = testServers[i].getRecievedCount();
//						tableData[i][3] = (testServers[i].getRecievedCount() * 100
//								/ expectedCountMap.get(testServers[i].getServerId())) + "%";
//						tableData[i][4] = testServers[i].getResolvedCount();
//						tableData[i][5] = (testServers[i].getResolvedCount() * 100 / Server.GRID_SIZE) + "%";
//				}
				cui.setTableInfo(tableData);
		}

		private void generateKeySpace(long[] tIds, Map<Long, Long> ks) {
				long i;
				int tc = 0;
				for (i = 0; tc < Server.GRID_SIZE; i++) {
						if (i > tIds[tc]) {
								tc++;
								if (tc == tIds.length) {
										break;
								}
						}
						ks.put(i, tIds[tc]);
				}
				for (; i < Server.GRID_SIZE; i++) {
						ks.put(i, tIds[0]);
				}
		}

		private void testKeySpaces() {
				Map<Long, Long> tempKeySpace;
				long failCount;
				for (int i = 0; i < NUM_OF_AUTO_TESTING_NODES; i++) {
						failCount = 0;
//						tempKeySpace = testServers[i].getKeySpace();
						for (long j = 0; j < Server.GRID_SIZE; j++) {
//								if (tempKeySpace.get(j) != keySpace.get(j)) {
//										failCount++;
//								}
						}
						if (failCount == 0) {
//								System.out.println("Tester " + testServers[i].getServerId() + "'s key space is matching 100%");
						} else {
//								System.out.println("Tester " + testServers[i].getServerId()
//										+ "'s key space is invalid " + (failCount * 100 / Server.GRID_SIZE) + "%");
						}
//						System.out.println("Expected map");
//						printKeySpace(keySpace);
//						System.out.println("Resolved map");
//						printKeySpace(tempKeySpace);
				}
		}

		private void generateExpectedCountMap(long[] testIds) {
				expectedCountMap.clear();
				expectedCountMap.put(testIds[0],
						(Server.GRID_SIZE - testIds[testIds.length - 1] + testIds[0]) * NUM_OF_AUTO_TESTING_NODES);
				for (int i = 1; i < testIds.length; i++) {
						expectedCountMap.put(testIds[i], (testIds[i] - testIds[i - 1]) * NUM_OF_AUTO_TESTING_NODES);
				}

		}

		/**
		 * 
		 */
		public void startRingTest() {
				Set<Short> keySet = testServers.keySet();
				for (Short key : keySet) {
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
//				System.out.println(completedCount + " of " + NUM_OF_TESTING_NODES + " are stable... \t\t" + id);
				cui.setStatus(completedCount + " of " + NUM_OF_TESTING_NODES + " are stable...");
				if (completedCount == NUM_OF_TESTING_NODES) {
						startLock.release();
				}
		}

		/**
		 * 
		 * @param text
		 * @param destination
		 * @param requester
		 */
		public void send(String text, long destination, long requester) {
				Set<Short> keySet = testServers.keySet();
				for (Short key : keySet) {
						if (testServers.get(key).getServerId() == requester) {
								Message msg = new Message(null, MessageType.GET, 10);
								msg.setTargetKey(destination);
								msg.setMessage(text);
								testServers.get(key).getServer().send(msg);
						}
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
				int[] nodeIds = null, levels = null, classIds = null, locations = null;
				if (nodeIdsString.length() > 0) {
						temp = nodeIdsString.split(" ");
						nodeIds = new int[temp.length];
						for (int i = 0; i < nodeIds.length; i++) {
								nodeIds[i] = Integer.parseInt(temp[i]);
						}
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

		/**
		 * 
		 * @param val
		 */
		public static void testBufferSize(int val) {
				if (max_buffer_size < val) {
						max_buffer_size = val;
						System.out.println(max_buffer_size);
				}
		}
		public static int lifeTimeCounter = 0;

		public void trigger() {
				Set<Short> keySet = testServers.keySet();
				for (Short key : keySet) {
						testServers.get(key).trigger();
				}
		}
}
