/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.concurrent.ConcurrentHashMap;
import sinchana.thrift.Message;
import sinchana.thrift.DHTServer;
import sinchana.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.CONFIGURATIONS;
import sinchana.PortHandler;
import sinchana.Server;
import sinchana.test.TesterController;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.messagequeue.MessageQueue;

/**
 *
 * @author Hiru
 */
public class ThriftServer implements PortHandler {

	private Server server;
	private TServer tServer;
	private ConnectionPool connectionPool;
	private MessageQueue messageQueue = new MessageQueue(CONFIGURATIONS.OUTPUT_MESSAGE_BUFFER_SIZE, new MessageQueue.MessageEventHandler() {

		@Override
		public void process(Message message) {
			if (server.getSinchanaTestInterface() != null) {
				server.getSinchanaTestInterface().setOutMessageQueueSize(messageQueue.size());
			}
			if (server.getRoutingHandler().getFailedNodeSet().containsKey(message.destination.serverId)) {
				addBackToQueue(message);
			}
			Node prevStation = message.getStation();
			message.setStation(server);
			int result = send(message);
			message.setStation(prevStation);
			switch (result) {
				case PortHandler.ACCEPT_ERROR:
				case PortHandler.LOCAL_SERVER_ERROR:
					message.retryCount++;
					if (message.retryCount > CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES) {
						Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
								"Messaage is terminated as maximum number of retries is exceeded! :: " + message);
					} else {
						Logger.log(server.serverId, Logger.LEVEL_FINE, Logger.CLASS_THRIFT_SERVER, 3,
								"Messaage is added back to the queue :: " + message);
						queueMessage(message);
					}
					break;
				case PortHandler.REMOTE_SERVER_ERROR:
					message.retryCount++;
					if (message.retryCount > CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
						Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
								"Node " + message.destination + " is removed from the routing table!");
						server.getRoutingHandler().removeNode(message.destination);
						addBackToQueue(message);
					} else {
						Logger.log(server.serverId, Logger.LEVEL_FINE, Logger.CLASS_THRIFT_SERVER, 3,
								"Messaage is added back to the queue :: " + message);
						queueMessage(message);
					}
					break;
			}
		}
	}, CONFIGURATIONS.NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS);

	/**
	 * 
	 * @param svr
	 */
	public ThriftServer(Server svr) {
		this.server = svr;
		this.connectionPool = new ConnectionPool(svr);
	}

	private void addBackToQueue(Message message) {
		if (message.type == MessageType.JOIN && message.source.serverId.equals(this.server.serverId)) {
			Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
					"Join failed 'cos " + message.destination + " is unreacheble!");
		} else if (message.type != MessageType.DISCOVER_NEIGHBOURS) {
			message.lifetime++;
			server.getMessageHandler().queueMessage(message);
			Logger.log(server.serverId, Logger.LEVEL_FINE, Logger.CLASS_THRIFT_SERVER, 3,
					"Message destinated to  " + message.destination + " is added back to the queue.");
		}
	}

	/**
	 * 
	 */
	@Override
	public synchronized void startServer() {
		if (tServer == null || !tServer.isServing()) {
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						DHTServer.Processor processor = new DHTServer.Processor(new DHTServer.Iface() {

							/**
							 * This method will be called when a message is received and the message 
							 * will be passed as the argument. 
							 * @param message Message transfered to the this node.
							 * @return 
							 * @throws TException
							 */
							@Override
							public int transfer(Message message) throws TException {
								if (CONFIGURATIONS.ROUND_TIP_TIME != 0) {
									try {
										Thread.sleep(CONFIGURATIONS.ROUND_TIP_TIME);
									} catch (InterruptedException ex) {
										throw new RuntimeException(ex);
									}
								}
								if (server.getMessageHandler().queueMessage(message)) {
									return PortHandler.SUCCESS;
								}
								return PortHandler.ACCEPT_ERROR;
							}
						});
						int localPortId = Integer.parseInt(server.getAddress().split(":")[1]);
						TServerTransport serverTransport = new TServerSocket(localPortId);
						tServer = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));
						Logger.log(server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 0,
								"Starting the server on port " + localPortId);
						tServer.serve();
						Logger.log(server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
								"Server is shutting down...");
					} catch (TTransportException ex) {
						throw new RuntimeException(ex);
					}
				}
			});
			thread.start();
			while (tServer == null || !tServer.isServing()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
			}
		}
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
		messageQueue.reset();
		if (tServer != null && tServer.isServing()) {
			tServer.stop();
		}
		this.connectionPool.closeAllConnections();
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setServerIsRunning(false);
		}
	}

	/**
	 * 
	 * @param message
	 * @param destination
	 */
	@Override
	public void send(Message message, Node destination) {
		Message msg = message.deepCopy();
		msg.lifetime--;
		if (msg.lifetime < 0) {
			Logger.log(server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
					"Messaage is terminated as lifetime expired! :: " + message);
			return;
		}
		msg.setDestination(destination.deepCopy());
		msg.setRetryCount(0);
		boolean done = queueMessage(msg);
		while (!done && message.type != MessageType.DISCOVER_NEIGHBOURS) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException ex) {
				throw new RuntimeException(ex);
			}
			done = queueMessage(msg);
		}
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setOutMessageQueueSize(messageQueue.size());
		}
	}

	private boolean queueMessage(Message message) {
		if (this.messageQueue.queueMessage(message)) {
			Logger.log(this.server.serverId, Logger.LEVEL_FINE, Logger.CLASS_THRIFT_SERVER, 1,
					"Queued in transport buffer: " + message);
			return true;
		} else {
			Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 1,
					"Message is unacceptable 'cos transport buffer is full! " + message);
			return false;
		}
	}
	private ConcurrentHashMap<Long, Long> ttmap = new ConcurrentHashMap<Long, Long>();

	private int send(Message message) {
		try {
			DHTServer.Client client = connectionPool.getConnection(
					message.destination.serverId, message.destination.address);
			if (client == null) {
				return PortHandler.REMOTE_SERVER_ERROR;
			}
			synchronized (client) {
				ttmap.put(Thread.currentThread().getId(), Thread.currentThread().getId());
				if (ttmap.size() > CONFIGURATIONS.NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS) {
					throw new RuntimeException("This is unbelievable :P");
				}
				TesterController.setMaxCount(ttmap.size());
				int reply = client.transfer(message);
				ttmap.remove(Thread.currentThread().getId());
				return reply;
			}
		} catch (TTransportException ex) {
			Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 5,
					"Error " + ex.getClass().getName() + " - " + message.destination.address);
			return PortHandler.REMOTE_SERVER_ERROR;
		} catch (TException ex) {
			Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 5,
					"Error " + ex.getClass().getName() + " - " + message.destination.address);
			return PortHandler.REMOTE_SERVER_ERROR;
		} catch (Exception ex) {
			Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 5,
					"Error " + ex.getClass().getName() + " - " + message.destination.address);
			return PortHandler.REMOTE_SERVER_ERROR;
		}
	}
}
