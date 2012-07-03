/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package testPackage;

import dhtserverclient.RoutingHandler;
import dhtserverclient.thrift.Message;
import dhtserverclient.thrift.MessageType;
import dhtserverclient.thrift.Node;
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

		public static final String LOCAL_SERVER_ADDRESS = "localhost";
		public static final int LOCAL_PORT_ID_RANGE = 8000;
		public static final int LOCAL_FIRST_SERVER_ID = 0;
		public static final String REMOTE_SERVER_ADDRESS = "localhost";
		public static final int REMOTE_SERVER_ID = 0;
		public static final int REMOTE_SERVER_PORT_ID_RANGE = 8000;
		public static final int NUM_OF_TESTING_NODES = 5;
		public static final int NUM_OF_AUTO_TESTING_NODES = 1;
		public static final boolean GUI_ON = true;
		public static final int AUTO_TEST_TIMEOUT = 1000;
		private Tester[] testServers;
		private ControllerUI cui;
		private int completedCount = 0;
		private Semaphore startLock = new Semaphore(0);
		private int testServerUpCount = 0;
		private Map<Integer, Integer> expectedCountMap = new HashMap<Integer, Integer>();
		private int[] keySpace = new int[RoutingHandler.GRID_SIZE];

		public static void main(String[] args) {
				TesterController testerController = new TesterController();
		}

		private TesterController() {
				cui = new ControllerUI(this);
				cui.setVisible(true);
		}

		public void join(int id, int rid) {
				Node n = new Node();
				n.serverId = rid;
				n.portId = n.serverId + REMOTE_SERVER_PORT_ID_RANGE;
				n.address = REMOTE_SERVER_ADDRESS;
				new Tester(id, n, this);
//		new Server().startNode(sid, sid + LOCAL_PORT_ID_RANGE, LOCAL_SERVER_ADDRESS, n);
		}

		private int[] generateIds(int numberOfIds) {
				int temp;
				List<Integer> nodeIds = new ArrayList<Integer>();
				while (nodeIds.size() < numberOfIds - 1) {
						temp = (int) (Math.random() * RoutingHandler.GRID_SIZE);
						if (temp != 0 && !nodeIds.contains(temp)) {
								nodeIds.add(temp);
						}
				}
				int[] testIds = new int[numberOfIds];
				testIds[0] = 0;
				temp = 1;
				for (Integer i : nodeIds) {
						testIds[temp++] = i;
				}
				return testIds;
		}

		public void startNodeSet() {
				int[] testIds = this.generateIds(NUM_OF_TESTING_NODES);
//				int[] testIds = {
//						0, 7, 16, 32, 34, 37, 38, 101, 112, 115, 118,
//						143, 256, 278, 283, 385, 390, 409, 449, 492, 500,
//						508, 532, 544, 550, 559, 561, 584, 705, 771, 780,
//						934, 947, 968, 1001, 1018};
				Node n = new Node();
				n.serverId = LOCAL_FIRST_SERVER_ID;
				n.portId = LOCAL_FIRST_SERVER_ID + LOCAL_PORT_ID_RANGE;
				n.address = LOCAL_SERVER_ADDRESS;

				testServers = new Tester[NUM_OF_TESTING_NODES];
				testServers[0] = new Tester(0, null, this);
				for (int i = 1; i < testServers.length; i++) {
						testServers[i] = new Tester(testIds[i], n, this);
						testServerUpCount++;
//						cui.setStatus(completedCount + " of " + testServerUpCount + " are stable...");
				}

				Arrays.sort(testIds);
				expectedCountMap.put(0, (RoutingHandler.GRID_SIZE - testIds[testIds.length - 1]) * NUM_OF_AUTO_TESTING_NODES);
				for (int i = 1; i < testIds.length; i++) {
						expectedCountMap.put(testIds[i], (testIds[i] - testIds[i - 1]) * NUM_OF_AUTO_TESTING_NODES);
				}

				generateKeySpace(testIds, keySpace);
				for (Tester tester : testServers) {
						tester.setKeySpaceReal(keySpace);
						tester.startServer();
				}

				try {
						startLock.acquire();
				} catch (InterruptedException ex) {
						Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
				}
				Object[][] tableData = new Object[NUM_OF_TESTING_NODES][4];
				for (int i = 0; i < testServers.length; i++) {
						tableData[i][0] = testServers[i].getServerId();
						tableData[i][1] = testServers[i].getStartTime().get(Calendar.HOUR_OF_DAY)
								+ ":" + testServers[i].getStartTime().get(Calendar.MINUTE)
								+ ":" + testServers[i].getStartTime().get(Calendar.SECOND)
								+ ":" + testServers[i].getStartTime().get(Calendar.MILLISECOND);
						tableData[i][2] = testServers[i].getEndTime().get(Calendar.HOUR_OF_DAY)
								+ ":" + testServers[i].getEndTime().get(Calendar.MINUTE)
								+ ":" + testServers[i].getEndTime().get(Calendar.SECOND)
								+ ":" + testServers[i].getEndTime().get(Calendar.MILLISECOND);
						tableData[i][3] = testServers[i].getEndTime().getTimeInMillis()
								- testServers[i].getStartTime().getTimeInMillis();
				}
				String[] coloms = {"Node ID", "Start", "End", "Duration"};
				cui.setTableInfo(tableData, coloms);
		}

		public void startAutoTest() {
				for (int i = 0; i < NUM_OF_AUTO_TESTING_NODES; i++) {
						testServers[i].startTest();
				}
				int tCount = 0;
				boolean running = true;
				long timeNow;
				while (running) {
						try {
								Thread.sleep(AUTO_TEST_TIMEOUT);
						} catch (InterruptedException ex) {
								Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
						}
						running = false;
						timeNow = Calendar.getInstance().getTimeInMillis();
						tCount++;
						System.out.println(tCount);
						for (Tester tester : testServers) {
								if (timeNow - tester.getEndTime().getTimeInMillis() < 5 * AUTO_TEST_TIMEOUT) {
										running = true;
										break;
								}
						}
				}

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
				String[] coloms = {"Node ID", "Expected", "Recieved", "Precentage", "Resolved", "Precentage"};
				cui.setTableInfo(tableData, coloms);
				testKeySpaces();
		}

		private void generateKeySpace(int[] tIds, int[] ks) {
				int i, tc = 0;
				for (i = 1; tc < ks.length; i++) {
						if (i > tIds[tc]) {
								tc++;
								if (tc == tIds.length) {
										break;
								}
						}
						ks[i] = tIds[tc];
				}
				for (; i < ks.length; i++) {
						ks[i] = 0;
				}
				ks[0] = 0;
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
								System.out.println("Tester " + testServers[i].getServerId() + "'s key space is valid");
						} else {
								System.out.println("Tester " + testServers[i].getServerId()
										+ "'s key space is invalid " + (failCount * 100 / tempKeySpace.length) + "%");
								printKeySpace(keySpace);
								printKeySpace(tempKeySpace);
						}
				}
		}

		public void startRingTest() {
				testServers[0].startRingTest();
		}

		public synchronized void incrementCompletedCount() {
				completedCount++;
				System.out.println(completedCount + " of " + NUM_OF_TESTING_NODES + " are stable...");
//				cui.setStatus(completedCount + " of " + testServerUpCount + " are stable...");
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

		public void send(String text, int destination, int requester) {
				for (Tester tester : testServers) {
						if (tester.getServerId() == requester) {
								Message msg = new Message(null, MessageType.GET, 10);
								msg.setTargetKey(destination);
								msg.setMessage(text);
								tester.getServer().transferMessage(msg);
								break;
						}
				}
		}

		public void printLogs(String nodeIdsString, String typesString, String classIdsString, String locationsString) {
				String[] temp;
				temp = nodeIdsString.split(" ");
				System.out.println("lll " + temp.length);
				int[] nodeIds = new int[temp.length];
				for (int i = 0; i < nodeIds.length; i++) {
						nodeIds[i] = Integer.parseInt(temp[i]);
				}
				temp = typesString.split(" ");
				int[] levels = new int[temp.length];
				for (int i = 0; i < temp.length; i++) {
						levels[i] = Integer.parseInt(temp[i]);
				}
				temp = classIdsString.split(" ");
				int[] classIds = new int[temp.length];
				for (int i = 0; i < temp.length; i++) {
						classIds[i] = Integer.parseInt(temp[i]);
				}
				temp = locationsString.split(" ");
				int[] locations = new int[temp.length];
				for (int i = 0; i < temp.length; i++) {
						locations[i] = Integer.parseInt(temp[i]);
				}
				dhtserverclient.util.logging.Logger.print(nodeIds, levels, classIds, locations);
		}
}
