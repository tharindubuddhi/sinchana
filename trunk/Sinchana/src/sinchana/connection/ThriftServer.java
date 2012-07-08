/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.logging.Level;
import sinchana.thrift.Message;
import sinchana.thrift.DHTServer;
import sinchana.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.PortHandler;
import sinchana.Server;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class ThriftServer implements DHTServer.Iface, Runnable, PortHandler {

		private Server server;
		private TServer tServer;
		private boolean running;
		private ConnectionPool connectionPool;
                private Thread t;
                private boolean connectionSuccess;
                private int connectionStatus;
                private int connectionTryout = 0;
                private int connectionTimewait = 5000;
		public ThriftServer(Server server) {
				this.server = server;
				this.connectionPool = new ConnectionPool(server);
				this.running = false;
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
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 0,
								"Starting the server on port " + this.server.getPortId());
						this.running = true;
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setServerIsRunning(true);
						}
						tServer.serve();
						this.running = false;
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setServerIsRunning(false);
						}
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
								"Server is shutting down...");
				} catch (TTransportException ex) {
						ex.printStackTrace();
				}
		}

		@Override
		public void startServer() {
				if (!this.running) {
						new Thread(this).start();
				}
		}

		@Override
		public void stopServer() {
				if (this.running && this.tServer != null) {
						tServer.stop();
				}
		}

		@Override
		public synchronized int send(Message message, Node destination) {
				message.lifetime--;
				if (message.lifetime < 0) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
								"Messaage " + message + " is terminated as lifetime expired!");
						return 0;
				}
				message.station = this.server;                                
				TTransport transport = connectionPool.getConnection(destination.serverId, destination.address, destination.portId);
				
                                try {
						TProtocol protocol = new TBinaryProtocol(transport);
						DHTServer.Client client = new DHTServer.Client(protocol);
						if (client.transfer(message)) {
								connectionStatus = 1;
						} else {
                                                    reconnect(client,message);
								return 2;//retry count and queue
						}

				} catch (TTransportException ex) {
						if (ex.toString().split(":")[1].trim().equals("java.net.ConnectException")) {
								return 3;
						} else if (ex.toString().split(":")[1].trim().equals("java.net.SocketException")) {
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 4,
										"Falied to connect " + ex.toString() + destination.address + ":" + destination.portId + " " + ex.getMessage() + " :: " + message);
						}
						return 4;
				} catch (TException ex) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 5,
								"Falied to connect 1 " + destination.address + ":" + destination.portId);
						return 4;
				} catch (Exception ex) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 6,
								"Falied to connect 2 " + destination.address + ":" + destination.portId);
						ex.printStackTrace();
						return 5;
				}
                                return connectionStatus;

		}
                
                public synchronized void reconnect(final DHTServer.Client client, final Message msg){
                    
                    if(t!=null ){
                        t = new Thread(new Runnable() {

                @Override
                public void run() {
                    do{
                        try {
                            t.sleep(connectionTimewait);
                            connectionSuccess = client.transfer(msg);
                            connectionTimewait += 10000;
                        } catch (Exception ex) {
                            
                        }                                            
                    }while(!connectionSuccess && connectionTryout < 5);
                }
            });
                        t.start();
                    }
                }
                
}
