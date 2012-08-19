/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
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
import sinchana.util.tools.ByteArrays;

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
	private final SinchanaServer server;
	private final SynchronousQueue<Message> outputMessageQueue = new SynchronousQueue<Message>();
	private TServer tServer;
	private boolean running = false;
	private final Runnable outputMessageQueueProcessor = new Runnable() {

		@Override
		public void run() {
			while (running) {
				try {
					processMessage(outputMessageQueue.take());
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
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

	public void send(Message message, Node destination) {
		message.lifetime--;
		if (message.lifetime < 0) {
//			Logger.log(server.getNode(), Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 1,
//					"Messaage is terminated as lifetime expired! :: " + message);
			message.setError("Messaage is terminated as lifetime expired!");
			message.setDestination(message.source);
			message.setDestinationId(message.source.getServerId());
			message.setType(MessageType.ERROR);
			message.setSuccess(false);
			message.setSource(this.server.getNode());
		} else {
			message.setDestination(destination.deepCopy());
		}
		try {
			outputMessageQueue.put(message);
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}

	}

	private boolean isRoutable(Message message) {
		switch (message.type) {
			case JOIN:
				if (Arrays.equals(message.source.getServerId(), this.server.getNode().getServerId())) {
					return false;
				}
			case TEST_RING:
			case REQUEST:
			case STORE_DATA:
			case DELETE_DATA:
			case GET_DATA:
			case GET_SERVICE:
				return true;
			case DISCOVER_NEIGHBORS:
				return false;
			case ERROR:
			case RESPONSE:
			case RESPONSE_DATA:
			case RESPONSE_SERVICE:
			case ACKNOWLEDGE_DATA_STORE:
			case ACKNOWLEDGE_DATA_REMOVE:
			default:
				System.out.println("Discarded: " + message);
				return false;
		}
	}

	private void processMessage(Message message) {
		int tries = 0;
		while (tries < CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES + 1) {
			tries++;
			if (tries > CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES) {
				if (!isRoutable(message)) {
					return;
				}
				message.setError("Messaage is terminated as maximum number of retries is exceeded!");
				message.setDestination(message.source);
				message.setDestinationId(message.source.getServerId());
				message.setType(MessageType.ERROR);
				message.setSuccess(false);
				message.setSource(this.server.getNode());
				tries = 0;
			}
			Node prevStation = message.getStation();
			message.setStation(server.getNode());
			int result = send(message);
			message.setStation(prevStation);
			switch (result) {
				case SUCCESS:
					return;
				case ACCEPT_ERROR:
				case LOCAL_SERVER_ERROR:
				case REMOTE_SERVER_ERROR:
					break;
				case REMOTE_SERVER_ERROR_FAILURE:
					boolean updated = server.getRoutingHandler().updateTable(message.destination, false);
					if (updated) {
						Logger.log(server.getNode(), Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 1,
								"Node " + ByteArrays.toReadableString(message.destination.serverId).toUpperCase()
								+ " @ "
								+ message.destination.address + " is removed from the routing table!");
						server.getRoutingHandler().triggerOptimize();
					}
					if (isRoutable(message)) {
						if (!this.server.getMessageHandler().queueMessage(message, true)) {
							throw new RuntimeException();
						}
					}
					return;
			}
		}
		throw new RuntimeException("This is unacceptable :P");
	}

	private int send(Message message) {
		Connection connection = this.server.getConnectionPool().getConnection(message.destination);
		connection.open();
		if (!connection.isOpened()) {
			if (connection.getNumOfOpenTries() >= CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
				connection.failedPermenently();
				return REMOTE_SERVER_ERROR_FAILURE;
			}
			return REMOTE_SERVER_ERROR;
		}
		try {
			synchronized (connection) {
				return connection.getClient().transfer(message);
			}
		} catch (Exception ex) {
			if (connection.getNumOfOpenTries() >= CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
				connection.failedPermenently();
				return REMOTE_SERVER_ERROR_FAILURE;
			}
			connection.failed();
			return REMOTE_SERVER_ERROR;
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
						Logger.log(server.getNode(), Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
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
				}
			}
		}
		running = true;
		for (int i = 0; i < CONFIGURATIONS.NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS; i++) {
			new Thread(outputMessageQueueProcessor).start();
		}
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setServerIsRunning(true);
		}
	}

	public void stopServer() {
		running = false;
		if (tServer != null && tServer.isServing()) {
			tServer.stop();
		}
		this.server.getConnectionPool().closeAllConnections();
		if (this.server.getSinchanaTestInterface() != null) {
			this.server.getSinchanaTestInterface().setServerIsRunning(false);
		}
	}
}
