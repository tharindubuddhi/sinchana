/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import sinchana.RoutingHandler;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Hiru
 */
public class TesterController {

		/**
		 * 
		 */
		public static final String LOCAL_SERVER_ADDRESS = "localhost";
//		public static final String LOCAL_SERVER_ADDRESS = "10.8.108.59";
		/**
		 * 
		 */
		public static final int LOCAL_PORT_ID_RANGE = 8000;
		/**
		 * 
		 */
		public static final String REMOTE_SERVER_ADDRESS = "localhost";
		/**
		 * 
		 */
		public static final int REMOTE_SERVER_ID = 0;
		/**
		 * 
		 */
		public static final int REMOTE_SERVER_PORT_ID = 8000 + REMOTE_SERVER_ID;
		/**
		 * 
		 */
		public static int NUM_OF_TESTING_NODES = 2;
		/**
		 * 
		 */
		public static int NUM_OF_AUTO_TESTING_NODES = 1;
		/**
		 * 
		 */
		public static final boolean GUI_ON = true;
		/**
		 * 
		 */
		public static final int AUTO_TEST_TIMEOUT = 2;
		public static final int ROUND_TIP_TIME = 0;
		/**
		 * 
		 */
		public static int max_buffer_size = 0;
		
		private Tester[] testServers;
		private ControllerUI cui;
		private int completedCount = 0;
		private Semaphore startLock = new Semaphore(0);
		private Map<Integer, Integer> expectedCountMap = new HashMap<Integer, Integer>();
		private int[] keySpace = new int[RoutingHandler.GRID_SIZE];
		private int[] testIds;

		/**
		 * 
		 * @param args
		 */
		public static void main(String[] args) {
				TesterController testerController = new TesterController();
		}

		private TesterController() {
				cui = new ControllerUI(this);
				cui.setVisible(true);
		}

		/**
		 * 
		 * @param id
		 * @param rid
		 */
		public void join(int id, int rid) {
				Node n = new Node();
				n.serverId = REMOTE_SERVER_ID;
				n.portId = REMOTE_SERVER_PORT_ID;
				n.address = REMOTE_SERVER_ADDRESS;
				new Tester(id, n, this);
//		new Server().startNode(sid, sid + LOCAL_PORT_ID_RANGE, LOCAL_SERVER_ADDRESS, n);
		}

		private int[] generateIds(int numberOfIds) {
				int temp;
				List<Integer> nodeIds = new ArrayList<Integer>();
				nodeIds.add(REMOTE_SERVER_ID);
				while (nodeIds.size() < numberOfIds) {
						temp = (int) (Math.random() * RoutingHandler.GRID_SIZE);
						if (temp != 0 && !nodeIds.contains(temp)) {
								nodeIds.add(temp);
						}
				}
				int[] tIds = new int[numberOfIds];
				temp = 0;
				for (Integer i : nodeIds) {
						tIds[temp++] = i;
				}
				return tIds;
		}

		public void startNodes(String nodeIds) {
				Node n = new Node();
				n.serverId = REMOTE_SERVER_ID;
				n.portId = REMOTE_SERVER_PORT_ID;
				n.address = REMOTE_SERVER_ADDRESS;
				String[] nids = nodeIds.trim().split("-");
				for (String string : nids) {
						int id = Integer.parseInt(string);
						Tester tester = new Tester(id, n, this);
						tester.resetTester();
						tester.startServer();
				}
		}

		/**
		 * 
		 * @param numOfTesters
		 */
		public void startNodeSet(int numOfTesters) {
				String[] coloms = {"Node ID", "Start", "End", "Duration"};
				cui.initTableInfo(coloms);
				try {
						NUM_OF_TESTING_NODES = numOfTesters;

						testIds = this.generateIds(NUM_OF_TESTING_NODES);

						for (int i = 0; i < testIds.length; i++) {
								int j = testIds[i];
								System.out.print(j + " ");
						}
						System.out.println("");

						Node n = new Node();
						n.serverId = REMOTE_SERVER_ID;
						n.portId = REMOTE_SERVER_PORT_ID;
						n.address = REMOTE_SERVER_ADDRESS;

						testServers = new Tester[NUM_OF_TESTING_NODES];
						for (int i = 0; i < testServers.length; i++) {
								testServers[i] = new Tester(testIds[i], n, this);
						}

						Arrays.sort(testIds);
						generateKeySpace(testIds, keySpace);
						generateExpectedCountMap(testIds);

						for (int i = 0; i < testServers.length; i++) {
								testServers[i].setExpectedCount(expectedCountMap.get(testServers[i].getServerId()));
								testServers[i].setRealKeySpace(keySpace);
								testServers[i].resetTester();
								testServers[i].startServer();
						}

						startLock.acquire();

						Object[][] tableData = new Object[NUM_OF_TESTING_NODES][4];
						for (int i = 0; i < testServers.length; i++) {
								tableData[i][0] = testServers[i].getServerId();
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
						}
						cui.setTableInfo(tableData);
				} catch (InterruptedException ex) {
						Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
				}
		}

		/**
		 * 
		 * @param numOfAutoTesters
		 */
		public void startAutoTest(int numOfAutoTesters) {
				String[] coloms = {"Node ID", "Expected", "Recieved", "Precentage", "Resolved", "Precentage"};
				cui.initTableInfo(coloms);

				NUM_OF_AUTO_TESTING_NODES = numOfAutoTesters;
				Arrays.sort(testIds);
				generateKeySpace(testIds, keySpace);
				generateExpectedCountMap(testIds);

				for (int i = 0; i < NUM_OF_TESTING_NODES; i++) {
						testServers[i].setExpectedCount(expectedCountMap.get(testServers[i].getServerId()));
						testServers[i].setRealKeySpace(keySpace);
						testServers[i].resetTester();
				}
				for (int i = 0; i < NUM_OF_AUTO_TESTING_NODES; i++) {
						testServers[i].startTest();
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
						for (Tester tester : testServers) {
								if (timeNow - tester.getEndTime().getTimeInMillis() < AUTO_TEST_TIMEOUT * 1000) {
										running = true;
										break;
								}
						}
				}
				setAutoTestTableInfo();
				testKeySpaces();
		}

		private void setAutoTestTableInfo() {
				Object[][] tableData = new Object[NUM_OF_TESTING_NODES][6];
				for (int i = 0; i < testServers.length; i++) {
						tableData[i][0] = testServers[i].getServerId();
						tableData[i][1] = expectedCountMap.get(testServers[i].getServerId());
						tableData[i][2] = testServers[i].getRecievedCount();
						tableData[i][3] = (testServers[i].getRecievedCount() * 100
								/ expectedCountMap.get(testServers[i].getServerId())) + "%";
						tableData[i][4] = testServers[i].getResolvedCount();
						tableData[i][5] = (testServers[i].getResolvedCount() * 100 / RoutingHandler.GRID_SIZE) + "%";
				}
				cui.setTableInfo(tableData);
		}

		private void generateKeySpace(int[] tIds, int[] ks) {
				int i, tc = 0;
				for (i = 0; tc < ks.length; i++) {
						if (i > tIds[tc]) {
								tc++;
								if (tc == tIds.length) {
										break;
								}
						}
						ks[i] = tIds[tc];
				}
				for (; i < ks.length; i++) {
						ks[i] = tIds[0];
				}
		}

		private void testKeySpaces() {
				int[] tempKeySpace;
				int failCount;
				for (int i = 0; i < NUM_OF_AUTO_TESTING_NODES; i++) {
						failCount = 0;
						tempKeySpace = testServers[i].getKeySpace();
						for (int j = 0; j < tempKeySpace.length; j++) {
								if (tempKeySpace[j] != keySpace[j]) {
										failCount++;
								}
						}
						if (failCount == 0) {
								System.out.println("Tester " + testServers[i].getServerId() + "'s key space is matching 100%");
						} else {
								System.out.println("Tester " + testServers[i].getServerId()
										+ "'s key space is invalid " + (failCount * 100 / tempKeySpace.length) + "%");
						}
//						System.out.println("Expected map");
//						printKeySpace(keySpace);
//						System.out.println("Resolved map");
//						printKeySpace(tempKeySpace);
				}
		}

		private void generateExpectedCountMap(int[] testIds) {
				expectedCountMap.clear();
				expectedCountMap.put(testIds[0],
						(RoutingHandler.GRID_SIZE - testIds[testIds.length - 1] + testIds[0]) * NUM_OF_AUTO_TESTING_NODES);
				for (int i = 1; i < testIds.length; i++) {
						expectedCountMap.put(testIds[i], (testIds[i] - testIds[i - 1]) * NUM_OF_AUTO_TESTING_NODES);
				}

		}

		/**
		 * 
		 */
		public void startRingTest() {
				testServers[0].startRingTest();
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

		private void printKeySpace(int[] ks) {
				StringBuffer sb = new StringBuffer("");
				for (int i = 0; i < ks.length; i++) {
						sb.append(" ").append(ks[i]);
				}
				System.out.println(sb);
		}

		/**
		 * 
		 * @param text
		 * @param destination
		 * @param requester
		 */
		public void send(String text, int destination, int requester) {
				for (Tester tester : testServers) {
						if (tester.getServerId() == requester) {
								Message msg = new Message(null, MessageType.GET, 10);
								msg.setTargetKey(destination);
								msg.setMessage(text);
								tester.getServer().send(msg);
								break;
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
		public void printLogs(String nodeIdsString, String typesString, String classIdsString, String locationsString) {
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
				sinchana.util.logging.Logger.print(nodeIds, levels, classIds, locations);
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
}
