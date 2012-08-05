/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import sinchana.Server;
import sinchana.SinchanaInterface;
import sinchana.SinchanaTestInterface;
import sinchana.chord.FingerTableEntry;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.logging.Logger;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 *
 * @author Hiru
 */
public class Tester implements SinchanaTestInterface, Runnable {

    private Server server;
    private short testId;
    private ServerUI gui = null;
    private TesterController testerController;
    private Semaphore threadLock = new Semaphore(0);
    private boolean running = false;
    private long numOfTestingMessages = 0;
    private TestService testService = null;
    /**
     * 
     * @param serverId
     * @param anotherNode
     * @param tc
     */
    public Tester(short testId, TesterController tc) {
        try {
            this.testId = testId;
            this.testerController = tc;
            InetAddress[] ip = InetAddress.getAllByName("localhost");
            server = new Server(ip[0],
                    (short) (testId + TesterController.LOCAL_PORT_ID_RANGE));
            Node node = getRemoteNode(server.serverId,
                    server.address, server.portId);
            server.setAnotherNode(node);
            server.registerSinchanaInterface(new SinchanaInterface() {

                @Override
                public Message request(Message message) {
                    Logger.log(server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 2,
                            "Recieved REQUEST message : " + message);
                    requestCount++;
                    System.out.println("im here.. in the request");
                    return null;
                }

                @Override
                public void response(Message message) {
                    Logger.log(server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 2,
                            "Recieved RESPONSE message : " + message);
                }

                @Override
                public void error(Message message) {
                    Logger.log(server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 2,
                            "Recieved ERROR message : " + message);
                }
            });
            server.registerSinchanaTestInterface(this);
            server.startServer();
            testService = new TestService(this, tc);
            if (TesterController.GUI_ON) {
                this.gui = new ServerUI(this);
            }
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 
     */
    public void startServer() {
        Thread thread = new Thread(this);
        thread.start();
        this.running = true;
    }

    /**
     * 
     */
    public void stopServer() {
        server.stopServer();
        this.running = false;
    }

    /**
     * 
     */
    public void startTest(long numOfTestingMessages) {
        this.numOfTestingMessages = numOfTestingMessages;
        threadLock.release();
    }

    /**
     * 
     */
    public void startRingTest() {
        Message msg = new Message(this.server, MessageType.TEST_RING, Server.MESSAGE_LIFETIME);
        msg.setMessage("");
        this.server.send(msg);
    }

    @Override
    public void run() {
        try {
            if (this.gui != null) {
                this.gui.setServerId(server.serverId);
                this.gui.setVisible(true);
            }
            server.join();
            while (true) {
                threadLock.acquire();
                String randomDestination;
                while (numOfTestingMessages > 0) {
                    randomDestination = "" + (long) (Math.random() * Server.GRID_SIZE.longValue());
                    server.send(randomDestination, "where are you?");
                    numOfTestingMessages--;
                }
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 
     * @param isStable
     */
    @Override
    public void setStable(boolean isStable) {
        if (isStable) {
            Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_TESTER, 4,
                    this.server.serverId + " is now stable!");
            if (this.gui != null) {
                this.gui.setMessage("stabilized!");
            }
            testerController.incrementCompletedCount(this.testId);
        }
    }

    /**
     * 
     * @param predecessor
     */
    @Override
    public void setPredecessor(Node predecessor) {
        if (this.gui != null) {
            this.gui.setPredecessorId(predecessor != null ? predecessor.serverId : "n/a");
        }
    }

    /**
     * 
     * @param successor
     */
    @Override
    public void setSuccessor(Node successor) {
        if (this.gui != null) {
            this.gui.setSuccessorId(successor != null ? successor.serverId : "n/a");
        }
    }

    /**
     * 
     * @param fingerTableEntrys
     */
    @Override
    public void setRoutingTable(FingerTableEntry[] fingerTableEntrys) {
        if (this.gui != null) {
            this.gui.setTableInfo(fingerTableEntrys);
        }
    }

    /**
     * 
     * @param status
     */
    @Override
    public void setStatus(String status) {
        if (this.gui != null) {
            this.gui.setMessage(status);
        }
    }

    /**
     * 
     * @return
     */
    public String getServerId() {
        return server.serverId;
    }

    public int getTestId() {
        return testId;
    }

    /**
     * 
     * @return
     */
    public Server getServer() {
        return server;
    }

    public boolean isRunning() {
        return running;
    }

    public ServerUI getGui() {
        return gui;
    }

    /**
     * 
     * @param isRunning
     */
    @Override
    public void setServerIsRunning(boolean isRunning) {
        if (this.gui != null) {
            this.gui.setServerRunning(isRunning);
        }
    }

    public void send(String dest, String msg) {
        this.server.send(dest, msg);
    }

    @Override
    public boolean equals(Object obj) {
        return this.testId == ((Tester) obj).testId;
    }

    @Override
    public int hashCode() {
        return this.server.serverId.hashCode();
    }

    private Node getRemoteNode(String serverId, String address, short portId) {
        if (TesterController.USE_REMOTE_CACHE_SERVER) {
            try {
                URL url = new URL("http://cseanremo.appspot.com/remoteip?"
                        + "sid=" + serverId
                        + "&url=" + address
                        + "&pid=" + portId);
                URLConnection yc = url.openConnection();
                InputStreamReader isr = new InputStreamReader(yc.getInputStream());
                BufferedReader in = new BufferedReader(isr);
                String resp = in.readLine();
                System.out.println(testId + ":\tresp: " + resp);
                if (resp == null || resp.equalsIgnoreCase("null")) {
                    Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_TESTER, 3,
                            "Error in getting remote server details.");
                    return null;
                }
                if (!resp.equalsIgnoreCase("n/a")) {
                    Node node = new Node();
                    node.serverId = resp.split(":")[0];
                    node.address = resp.split(":")[1];
                    node.portId = Short.parseShort(resp.split(":")[2]);
                    in.close();
                    return node;
                }
            } catch (Exception e) {
                throw new RuntimeException("Invalid response from the cache server!", e);
            }
            return null;
        } else {
            Node node = new Node(serverId, address, portId);
            return LocalCacheServer.getRemoteNode(node);
        }
    }

    void trigger() {
        this.server.trigger();
    }
    private long inputMessageCount = 0;
    private long avarageInputMessageQueueSize = 0;
    private long inputMessageQueueTimesCount = 0;
    private long avarageOutputMessageQueueSize = 0;
    private long outputMessageQueueTimesCount = 0;
    private long requestCount = 0;

    @Override
    public void incIncomingMessageCount() {
        inputMessageCount++;
    }

    @Override
    public void setMessageQueueSize(int size) {
        avarageInputMessageQueueSize += size;
        inputMessageQueueTimesCount++;
    }

    @Override
    public void setOutMessageQueueSize(int size) {
        avarageOutputMessageQueueSize += size;
        outputMessageQueueTimesCount++;
    }

    public long[] getTestData() {
        long[] data = new long[4];
        data[0] = inputMessageCount;
        data[1] = inputMessageQueueTimesCount == 0 ? 0 : (avarageInputMessageQueueSize / inputMessageQueueTimesCount);
        data[2] = outputMessageQueueTimesCount == 0 ? 0 : (avarageOutputMessageQueueSize / outputMessageQueueTimesCount);
        data[3] = requestCount;
        inputMessageCount = 0;
        avarageInputMessageQueueSize = 0;
        inputMessageQueueTimesCount = 0;
        avarageOutputMessageQueueSize = 0;
        outputMessageQueueTimesCount = 0;
        requestCount = 0;
        return data;
    }
}
