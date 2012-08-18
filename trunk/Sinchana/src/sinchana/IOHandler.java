/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
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

/**
 *
 * @author Hiru
 */
public class IOHandler {

	public static final int ACCEPT_ERROR = 0;
	public static final int LOCAL_SERVER_ERROR = 1;
	public static final int REMOTE_SERVER_ERROR = 2;
	public static final int REMOTE_SERVER_ERROR_FAILURE = 3;
	public static final int SUCCESS = 4;
	private final int BUFFER_LIMIT = CONFIGURATIONS.OUTPUT_MESSAGE_BUFFER_SIZE * 95 / 100;
	private final SinchanaServer server;
	private TServer tServer;
	private boolean running = false;
	private final ArrayBlockingQueue<Message> outGoingMessageQueue = new ArrayBlockingQueue<Message>(CONFIGURATIONS.OUTPUT_MESSAGE_BUFFER_SIZE);
	private final Runnable outGoingMessageQueueProcessor = new Runnable() {

		@Override
		public void run() {
			while (running) {
				try {
					if (server.getSinchanaTestInterface() != null) {
						server.getSinchanaTestInterface().setOutMessageQueueSize(outGoingMessageQueue.size());
					}
					Message message = outGoingMessageQueue.take();
					if (server.getConnectionPool().hasReportFailed(message.destination)) {
						addBackToQueue(message);
						return;
					}
					Node prevStation = message.getStation();
					message.setStation(server.getNode());
					int result = send(message);
					message.setStation(prevStation);
					switch (result) {
						case ACCEPT_ERROR:
						case LOCAL_SERVER_ERROR:
							message.retryCount++;
							if (CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES != -1 && message.retryCount > CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES) {
								Logger.log(server.getNode(), Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
										"Messaage is terminated as maximum number of retries is exceeded! :: " + message);
							} else {
								queueMessage(message);
							}
							break;
						case REMOTE_SERVER_ERROR:
							queueMessage(message);
							break;
						case REMOTE_SERVER_ERROR_FAILURE:
							Logger.log(server.getNode(), Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
									"Node " + message.destination + " is removed from the routing table!");
							boolean updated = server.getRoutingHandler().updateTable(message.destination, false);
							if (updated) {
								server.getRoutingHandler().optimize();
							}
							addBackToQueue(message);
							break;
					}
				} catch (InterruptedException ex) {
				}
			}
		}
	};

	/**
	 * 
	 * @param svr
	 */
	public IOHandler(SinchanaServer svr) {
		this.server = svr;
	}

	private int send(Message message) {
		Connection connection = this.server.getConnectionPool().getConnection(message.destination);
		connection.open();
		if (!connection.isOpened()) {
			if (connection.getNumOfOpenTries() >= CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
				connection.failed();
				return REMOTE_SERVER_ERROR_FAILURE;
			}
			return REMOTE_SERVER_ERROR;
		}
		try {
			synchronized (connection) {
				return connection.getClient().transfer(message);
			}
		} catch (Exception ex) {
			connection.close();
			return REMOTE_SERVER_ERROR;
		}
	}

	private void addBackToQueue(Message message) {
		switch (message.type) {
			case JOIN:
				if (Arrays.equals(message.source.getServerId(), this.server.getNode().getServerId())) {
					Logger.log(server.getNode(), Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
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
				server.getMessageHandler().queueMessage(message);
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

	public synchronized void startServer() {
		if (tServer == null || !tServer.isServing()) {
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						DHTServer.Processor processor = new DHTServer.Processor(new ThriftServerImpl(server));
						int localPortId = Integer.parseInt(server.getNode().getAddress().split(":")[1]);
						TServerTransport serverTransport = new TServerSocket(localPortId);
						tServer = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));
						Logger.log(server.getNode(), Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 0,
								"Starting the server on port " + localPortId);
						tServer.serve();
						Logger.log(server.getNode(), Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
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
		running = true;
		for (int i = 0; i < CONFIGURATIONS.NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS; i++) {
			new Thread(outGoingMessageQueueProcessor).start();
		}
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setServerIsRunning(true);
		}
	}

	public void stopServer() {
		running = false;
		outGoingMessageQueue.clear();
		if (tServer != null && tServer.isServing()) {
			tServer.stop();
		}
		this.server.getConnectionPool().closeAllConnections();
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setServerIsRunning(false);
		}
	}

	public void send(Message message, Node destination) {
		message.lifetime--;
		if (message.lifetime < 0) {
			Logger.log(server.getNode(), Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
					"Messaage is terminated as lifetime expired! :: " + message);
			return;
		}
		message.setDestination(destination.deepCopy());
		message.setRetryCount(0);
		queueMessage(message);
	}

	private void queueMessage(Message message) {
		try {
			this.outGoingMessageQueue.put(message);
			if (this.server.getSinchanaTestInterface() != null) {
				this.server.getSinchanaTestInterface().setOutMessageQueueSize(outGoingMessageQueue.size());
			}
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}

	public boolean hasReachedLimit() {
		return outGoingMessageQueue.size() > BUFFER_LIMIT;
	}
}
