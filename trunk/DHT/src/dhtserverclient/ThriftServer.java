/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dhtserverclient;

import dhtserverclient.thrift.Message;
import dhtserverclient.thrift.DHTServer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author Hiru
 */
public class ThriftServer implements DHTServer.Iface, Runnable, PortHandler {

    private Server server;
    TServer tServer;

    public ThriftServer(Server server) {
        this.server = server;
    }

    /**
     * This method will be called when a message is received and the message 
     * will be passed as the argument. 
     * @param message Message transfered to the this node.
     * @throws TException
     */
    @Override
    public boolean transfer(Message message) throws TException {
        return this.server.getMessageHandler().queueMessage(message);
    }

    @Override
    public void run() {
        try {
            DHTServer.Processor processor = new DHTServer.Processor(this);
            TServerTransport serverTransport = new TServerSocket(this.server.getPortId());
            tServer = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(processor));
            Logger.getLogger(Server.class.getName()).logp(Level.INFO,
                    Server.class.getName(), "Server " + this.server.getServerId(),
                    "Starting the server on port " + this.server.getPortId());
            tServer.serve();
            System.out.println("im here");
        } catch (TTransportException ex) {
            Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void startServer() {
        new Thread(this).start();
    }

    @Override
    public synchronized boolean send(Message message, String address, int portId) {
        message.lifetime--;
        if (message.lifetime < 0) {
            Logger.getLogger(Server.class.getName()).logp(Level.WARNING,
                    Server.class.getName(), "Server " + this.server.serverId,
                    "Messaage " + message + " is terminated as lifetime expired!");
            return false;
        }
        message.station = this.server;
        TTransport transport = new TSocket(address, portId);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            DHTServer.Client client = new DHTServer.Client(protocol);
            boolean transfer = client.transfer(message);
            return true;
        } catch (TTransportException ex) {
//						Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("falied to connect " + address + ":" + portId);
//						ex.printStackTrace();
            //System.exit(1);
            return false;
        } catch (TException ex) {
//						Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("falied to connect 1 " + address + ":" + portId);
//						ex.printStackTrace();
            return false;
        } catch (Exception ex) {
            System.out.println("falied to connect 2 " + address + ":" + portId);
//						ex.printStackTrace();
            return false;
        } finally {
            transport.close();
        }

    }

    @Override
    public void downServer() {
        tServer.stop();

    }
}
