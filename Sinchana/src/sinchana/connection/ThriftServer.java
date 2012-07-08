/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

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
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.messagequeue.MessageEventHandler;
import sinchana.util.messagequeue.MessageQueue;

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
		private int connectionTimewait = 500;
		private MessageQueue messageQueue;
		private static final int MESSAGE_QUEUE_SIZE = 4096;
		private static final int NUM_OF_MAX_RETRIES = 3;
		
		public ThriftServer(Server s) {
				this.server = s;
				this.connectionPool = new ConnectionPool(s);
				this.running = false;
				messageQueue = new MessageQueue(MESSAGE_QUEUE_SIZE, new MessageEventHandler() {

						@Override
						public void process(Message message) {
								int result = send(message);
								switch (result) {
										case PortHandler.ACCEPT_ERROR:
												message.retryCount++;
												if (message.retryCount > NUM_OF_MAX_RETRIES) {
														Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
																"Messaage " + message + " is terminated as lifetime expired!");
												} else {
														queueMessage(message);
												}
												break;
										case PortHandler.MESSAGE_LIFE_TIME_EXPIRED:
												Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
														"Messaage " + message + " is terminated as lifetime expired!");
												break;
										case PortHandler.REMOTE_SERVER_ERROR:
												break;
										case PortHandler.LOCAL_SERVER_ERROR:
												break;
								}
						}
				});
				messageQueue.start();
		}

		/**
		 * This method will be called when a message is received and the message 
		 * will be passed as the argument. 
		 * @param message Message transfered to the this node.
		 * @throws TException
		 */
		@Override
		public int transfer(Message message) throws TException {
//				if (message.type == MessageType.GET && message.targetKey == 88) {
//						return PortHandler.ACCEPT_ERROR;
//				}
				if (this.server.getMessageHandler().queueMessage(message)) {
						return PortHandler.SUCCESS;
				}
				return PortHandler.ACCEPT_ERROR;
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
//				messageQueue.start();
		}

		@Override
		public void stopServer() {
				if (this.running && this.tServer != null) {
						tServer.stop();
				}
				this.connectionPool.closeAllConnections();
		}

		@Override
		public synchronized void send(Message message, Node destination) {
				message.setDestination(destination);
				message.setRetryCount(0);
//				this.send(message);
				this.queueMessage(message);
		}

		private void queueMessage(Message message) {
				if (!this.messageQueue.queueMessage(message)) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 1,
								"Message is unacceptable 'cos transport buffer is full! " + message);
				}
		}

		private int send(Message message) {
				message.lifetime--;
				if (message.lifetime < 0) {
						return PortHandler.MESSAGE_LIFE_TIME_EXPIRED;
				}
				message.station = this.server;
				TTransport transport = connectionPool.getConnection(
						message.destination.serverId, message.destination.address, message.destination.portId);
				if (transport == null) {
						return PortHandler.REMOTE_SERVER_ERROR;
				}

				try {
						TProtocol protocol = new TBinaryProtocol(transport);
						DHTServer.Client client = new DHTServer.Client(protocol);
						return client.transfer(message);


				} catch (TTransportException ex) {
						if (ex.toString().split(":")[1].trim().equals("java.net.ConnectException")) {
								return PortHandler.REMOTE_SERVER_ERROR;
						} else if (ex.toString().split(":")[1].trim().equals("java.net.SocketException")) {
								Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 4,
										"Falied to connect " + ex.toString() + message.destination.address + ":" + message.destination.portId + " " + ex.getMessage() + " :: " + message);
						}
						return PortHandler.REMOTE_SERVER_ERROR;
				} catch (TException ex) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 5,
								"Falied to connect 1 " + message.destination.address + ":" + message.destination.portId);
						return PortHandler.LOCAL_SERVER_ERROR;
				} catch (Exception ex) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 6,
								"Falied to connect 2 " + message.destination.address + ":" + message.destination.portId);
						ex.printStackTrace();
						return PortHandler.LOCAL_SERVER_ERROR;
				}
		}

		private synchronized void reconnect(final DHTServer.Client client, final Message msg) {
				if (t != null) {
						t = new Thread(new Runnable() {

								@Override
								public void run() {
										do {
												try {
														Thread.sleep(connectionTimewait);
//														connectionSuccess = client.transfer(msg);
														connectionTimewait += 10000;
														connectionTryout++;
												} catch (Exception ex) {
												}
										} while (!connectionSuccess && connectionTryout < 5);
								}
						});
						t.start();
				}
		}
}
