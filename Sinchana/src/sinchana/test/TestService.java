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
import java.util.concurrent.Semaphore;
import javax.xml.soap.Node;
import sinchana.Server;
import sinchana.SinchanaInterface;
import sinchana.SinchanaServiceInterface;
import sinchana.chord.FingerTableEntry;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.util.logging.Logger;

/**
 *
 * @author DELL
 */
public class TestService  {

    private Server server;
    private int testId;
    private ServerUI gui = null;
    private TesterController testerController;
    private Semaphore threadLock = new Semaphore(0);
    private boolean running = false;
    private long numOfTestingMessages = 0;

    /**
     * 
     * @param serverId
     * @param anotherNode
     * @param tc
     */
    public TestService(Tester tester,TesterController tc) {
                    
            this.testId = tester.getTestId();
            this.testerController = tc;
            this.server = tester.getServer();
            this.gui = tester.getGui();
            
            
            server.registerSinchanaServiceInterface(new SinchanaServiceInterface() {

                @Override
                public boolean publish(String key, String service) {
                    System.out.println(service+" published in the key "+key);
                    return true;
                }

                @Override
                public String get(String key) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                @Override
                public boolean remove(String key) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            });
        
    }
}
