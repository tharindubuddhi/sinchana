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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import sinchana.util.tools.CommonTools;

/**
 *
 * @author Hiru
 */
public class TesterController {

    public static final short LOCAL_PORT_ID_RANGE = 8000;
    public static short NUM_OF_TESTING_NODES = 0;
    public static short NUM_OF_AUTO_TESTING_NODES = 1;
    public static final boolean GUI_ON = false;
    public static final boolean USE_REMOTE_CACHE_SERVER = false;
    public static final int AUTO_TEST_TIMEOUT = 2;
    public static final int ROUND_TIP_TIME = 0;
    public static final int AUTO_TEST_MESSAGE_LIFE_TIME = 120;
    public static int max_buffer_size = 0;
    private final Map<Short, Tester> testServers = new HashMap<Short, Tester>();
    private final ControllerUI cui = new ControllerUI(this);
    private int completedCount = 0;
    private final Semaphore startLock = new Semaphore(0);
    private final Timer timer = new Timer();
    private final Timer timer2 = new Timer();
    private long numOfTestMessages = 0;

    /**
     * 
     * @param args
     */
    public static void main(String[] args) {
        if (TesterController.USE_REMOTE_CACHE_SERVER) {
            try {
                URL yahoo = new URL("http://cseanremo.appspot.com/remoteip?clear=true");
                URLConnection yc = yahoo.openConnection();
                InputStreamReader isr = new InputStreamReader(yc.getInputStream());
                isr.close();
            } catch (Exception e) {
                throw new RuntimeException("Error in clearing the cache server.", e);
            }
        } else {
            LocalCacheServer.clear();
        }
        new TesterController();
    }

    private TesterController() {
        cui.setVisible(true);
        timer.scheduleAtFixedRate(new TimerTask() {

            long totalMessageIncome, totalInputMessageQueue, totalOutputMessageQueue, totalResolves;
            long newTime, oldTime = Calendar.getInstance().getTimeInMillis();

            @Override
            public void run() {
                totalMessageIncome = 0;
                totalInputMessageQueue = 0;
                totalOutputMessageQueue = 0;
                totalResolves = 0;
                long[] testData;
                Set<Short> keySet = testServers.keySet();
                for (Short tid : keySet) {
                    testData = testServers.get(tid).getTestData();
                    totalMessageIncome += testData[0];
                    totalInputMessageQueue += testData[1];
                    totalOutputMessageQueue += testData[2];
                    totalResolves += testData[3];
                }
                newTime = Calendar.getInstance().getTimeInMillis();

                if (completedCount != 0) {
                    cui.setStat("IC: " + (totalMessageIncome / completedCount)
                            + "    IB: " + (totalInputMessageQueue / completedCount)
                            + "    OB: " + (totalOutputMessageQueue / completedCount)
                            + "    TR: " + totalResolves
                            + "    TP: " + (newTime > oldTime ? (totalResolves * 1000 / (newTime - oldTime)) : "INF") + "/S");
                }
                oldTime = newTime;
            }
        }, 1000, 1000);
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
    public void startNodeSet(short numOfTesters) {
        try {
            Tester tester;
            for (short i = NUM_OF_TESTING_NODES; i < NUM_OF_TESTING_NODES + numOfTesters; i++) {
                tester = new Tester(i, this);
                testServers.put(i, tester);
            }
            NUM_OF_TESTING_NODES += numOfTesters;
            String[] testServerIds = new String[NUM_OF_TESTING_NODES];

            for (short i = 0; i < NUM_OF_TESTING_NODES; i++) {
                testServerIds[i] = testServers.get(i).getServerId();
            }

            Arrays.sort(testServerIds);
            for (String id : testServerIds) {
                System.out.print(id + " ");
            }
            System.out.println("");
            Set<Short> keySet = testServers.keySet();
            for (Short key : keySet) {
                tester = testServers.get(key);
                if (!tester.isRunning()) {
                    tester.startServer();
                    System.out.println("Server " + tester.getServerId() + " is running...");
                }
            }
            startLock.acquire();
        } catch (InterruptedException ex) {
            Logger.getLogger(TesterController.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * 
     * @param numOfAutoTesters
     */
    public void startAutoTest(long numOfTestMessages) {
        this.numOfTestMessages = numOfTestMessages;
    }

    public void testMessages(long numOfTestMessages) {
        int numOfTestServers = testServers.size();
        short randomId;
        long randomAmount = 0;
        while (numOfTestMessages > 0) {
            randomId = (short) (Math.random() * numOfTestServers);
            if (numOfTestMessages > 250) {
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
    public void send(String text, String destination, String requester) {
        Set<Short> keySet = testServers.keySet();
        for (Short key : keySet) {
            if (testServers.get(key).getServerId().equals(requester)) {
                Message msg = new Message(null, MessageType.REQUEST, 10);
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

        short randomId;
        int randomAmount;
        int x = 0;
        while (noOfServices > 0) {

            randomId = (short) (Math.random() * testServers.size());
            randomAmount = (int) (Math.random() * noOfServices);
            noOfServices = noOfServices- randomAmount;
            while (randomAmount > 0) {
                testServers.get(randomId).getServer().publishService(keyArray[x], serviceArray[x]);
                x++;
                randomAmount--;
            }
        }
    }

    public void retrieveService() {
        short randomId;
        int randomAmount;
//                            while(noOfServices>0){
        randomId = (short) (Math.random() * testServers.size());
        randomAmount = (int) (Math.random() * serviceArray.length);
        for (int i = 0; i < serviceArray.length; i++) {

            testServers.get(randomId).getServer().getService(keyArray[i]);


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
        Set<Short> keySet = testServers.keySet();
        for (Short key : keySet) {
            testServers.get(key).trigger();
        }
    }
}
