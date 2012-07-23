/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.concurrent.Semaphore;
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
import sinchana.test.TesterController;
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
		private boolean runningLocal;
		private ConnectionPool connectionPool;
		private Semaphore serverRun = new Semaphore(0);
		private static final int MESSAGE_QUEUE_SIZE = 4196;
		private static final int NUM_OF_MAX_RETRIES = 3;
		private MessageQueue messageQueue = new MessageQueue(MESSAGE_QUEUE_SIZE, new MessageEventHandler() {

				@Override
				public void process(Message message) {
						int result = send(message.deepCopy());
						switch (result) {
								case PortHandler.ACCEPT_ERROR:
								case PortHandler.LOCAL_SERVER_ERROR:
										message.retryCount++;
										if (message.retryCount > NUM_OF_MAX_RETRIES) {
												Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
														"Messaage is terminated as maximum number of retries is exceeded! :: " + message);
										} else {
												Logger.log(server.serverId, Logger.LEVEL_FINE, Logger.CLASS_THRIFT_SERVER, 3,
														"Messaage is added back to the queue :: " + message);
												queueMessage(message);
										}
										break;
								case PortHandler.MESSAGE_LIFE_TIME_EXPIRED:
										Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
												"Messaage is terminated as lifetime expired! :: " + message);
										break;
								case PortHandler.REMOTE_SERVER_ERROR:
										server.getRoutingHandler().removeNode(message.destination);
										server.getRoutingHandler().optimize();
										break;
						}
				}
		});

		/**
		 * 
		 * @param svr
		 */
		public ThriftServer(Server svr) {
				this.server = svr;
				this.connectionPool = new ConnectionPool(svr);
				this.running = false;
				this.runningLocal = false;
		}

		/**
		 * This method will be called when a message is received and the message 
		 * will be passed as the argument. 
		 * @param message Message transfered to the this node.
		 * @return 
		 * @throws TException
		 */
		@Override
		public int transfer(Message message) throws TException {
				if (TesterController.ROUND_TIP_TIME != 0) {
						try {
								Thread.sleep(TesterController.ROUND_TIP_TIME);
						} catch (InterruptedException ex) {
								ex.printStackTrace();
						}
				}
				if (!runningLocal) {
						return PortHandler.REMOTE_SERVER_ERROR;
				}
//				if (message.type == sinchana.thrift.MessageType.GET && message.targetKey == 88) {
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
						serverRun.release();
						tServer.serve();
						this.running = false;
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
								"Server is shutting down...");
				} catch (TTransportException ex) {
						ex.printStackTrace();
				}
		}

		/**
		 * 
		 */
		@Override
		public void startServer() {
				if (!this.running) {
						new Thread(this).start();
				}
				try {
						serverRun.acquire();
				} catch (InterruptedException ex) {
						ex.printStackTrace();
				}

				this.runningLocal = true;
				messageQueue.start();
				if (this.server.getSinchanaTestInterface() != null) {
						this.server.getSinchanaTestInterface().setServerIsRunning(true);
				}
		}

		/**
		 * 
		 */
		@Override
		public void stopServer() {
//				if (this.running && this.tServer != null) {
//						tServer.stop();
//				}
				messageQueue.reset();
				this.runningLocal = false;
				this.connectionPool.closeAllConnections();
				Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
						"Server is shutting down...");
				if (this.server.getSinchanaTestInterface() != null) {
						this.server.getSinchanaTestInterface().setServerIsRunning(false);
				}
				serverRun.release();
		}

		/**
		 * 
		 * @param message
		 * @param destination
		 */
		@Override
		public void send(Message message, Node destination) {
				Message msg = message.deepCopy();
				msg.setDestination(destination.deepCopy());
				msg.setStation(this.server);
				msg.setRetryCount(0);
//				this.send(message);
				this.queueMessage(msg);
		}

		private void queueMessage(Message message) {
				if (!this.messageQueue.queueMessage(message)) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 1,
								"Message is unacceptable 'cos transport buffer is full! " + message);
				}
//				TesterController.testBufferSize(messageQueue.size());
		}

		private int send(Message message) {
				message.lifetime--;
				if (message.lifetime < 0) {
						return PortHandler.MESSAGE_LIFE_TIME_EXPIRED;
				}
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
}
