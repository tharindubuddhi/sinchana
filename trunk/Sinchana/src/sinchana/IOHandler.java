/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Semaphore;
import sinchana.connection.*;

import sinchana.thrift.Message;
import sinchana.thrift.DHTServer;
import sinchana.util.logging.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.messagequeue.MessageQueue;

/**
 *
 * @author Hiru
 */
public class IOHandler implements PortHandler {

	private final SinchanaServer server;
	private TServer tServer;
	private final Semaphore messageQueueLock = new Semaphore(0);
	private final MessageQueue messageQueue = new MessageQueue(CONFIGURATIONS.OUTPUT_MESSAGE_BUFFER_SIZE, new MessageQueue.MessageEventHandler() {

		@Override
		public void process(Message message) {
			if (server.getSinchanaTestInterface() != null) {
				server.getSinchanaTestInterface().setOutMessageQueueSize(messageQueue.size());
			}
			if (server.getConnectionPool().hasReportFailed(message.destination)) {
				addBackToQueue(message);
				return;
			}
			Node prevStation = message.getStation();
			message.setStation(server);
			int result = send(message);
			message.setStation(prevStation);
			switch (result) {
				case PortHandler.ACCEPT_ERROR:
				case PortHandler.LOCAL_SERVER_ERROR:
					message.retryCount++;
					if (CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES != -1 && message.retryCount > CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES) {
						Logger.log(server, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
								"Messaage is terminated as maximum number of retries is exceeded! :: " + message);
					} else {
						queueMessage(message, MessageQueue.PRIORITY_HIGH);
					}
					break;
				case PortHandler.REMOTE_SERVER_ERROR:
					queueMessage(message, MessageQueue.PRIORITY_HIGH);
					break;
				case PortHandler.REMOTE_SERVER_ERROR_FAILURE:
					Logger.log(server, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
							"Node " + message.destination + " is removed from the routing table!");
					boolean updated = server.getRoutingHandler().updateTable(message.destination, false);
					if (updated) {
						Message msg = new Message(MessageType.DISCOVER_NEIGHBORS, server, 2);
						Set<Node> failedNodes = server.getConnectionPool().getFailedNodes();
						msg.setFailedNodeSet(failedNodes);
						Set<Node> neighbourSet = server.getRoutingHandler().getNeighbourSet();
						for (Node node : neighbourSet) {
							send(msg, node);
						}
					}
					addBackToQueue(message);
					break;
			}
		}
	}, CONFIGURATIONS.NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS);

	/**
	 * 
	 * @param svr
	 */
	public IOHandler(SinchanaServer svr) {
		this.server = svr;
	}

	private void addBackToQueue(Message message) {
		switch (message.type) {
			case JOIN:
				if (Arrays.equals(message.source.getServerId(), this.server.getServerId())) {
					Logger.log(server, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
							"Join failed 'cos " + message.destination + " is unreacheble!");
					break;
				}
			case TEST_RING:
			case REQUEST:
			case STORE_DATA:
			case DELETE_DATA:
			case GET_DATA:
			case GET_SERVICE:
				message.lifetime++;
				server.getMessageHandler().queueMessage(message, MessageQueue.PRIORITY_HIGH);
				break;
			case DISCOVER_NEIGHBORS:
				break;
			case ERROR:
			case RESPONSE:
			case RESPONSE_DATA:
			case RESPONSE_SERVICE:
			case ACKNOWLEDGE_DATA_STORE:
			case ACKNOWLEDGE_DATA_REMOVE:
				break;
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
						DHTServer.Processor processor = new DHTServer.Processor(new ThriftServerImpl(server));
						int localPortId = Integer.parseInt(server.getAddress().split(":")[1]);
						TServerTransport serverTransport = new TServerSocket(localPortId);
						tServer = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));
						Logger.log(server, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 0,
								"Starting the server on port " + localPortId);
						tServer.serve();
						Logger.log(server, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
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
		this.server.getConnectionPool().closeAllConnections();
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
			Logger.log(server, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
					"Messaage is terminated as lifetime expired! :: " + message);
			return;
		}
		msg.setDestination(destination.deepCopy());
		msg.setRetryCount(0);
		queueMessage(msg, MessageQueue.PRIORITY_LOW);
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setOutMessageQueueSize(messageQueue.size());
		}
	}

	private boolean queueMessage(Message message, int priority) {
		return this.messageQueue.queueMessageAndWait(message, priority);
	}

	private int send(Message message) {
		Connection connection = this.server.getConnectionPool().getConnection(message.destination);
		connection.open();
		if (!connection.isOpened()) {
			if (connection.getNumOfOpenTries() >= CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
				connection.failed();
				return PortHandler.REMOTE_SERVER_ERROR_FAILURE;
			}
			return PortHandler.REMOTE_SERVER_ERROR;
		}
		try {
			synchronized (connection) {
				return connection.getClient().transfer(message);
			}
		} catch (Exception ex) {
			connection.close();
			return PortHandler.REMOTE_SERVER_ERROR;
		}
	}
}
