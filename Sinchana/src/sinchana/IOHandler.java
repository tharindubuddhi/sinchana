/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
import org.apache.thrift.TException;
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

	public static final int ACCEPT_ERROR = 1;
	public static final int SUCCESS = 2;
	private final SinchanaServer server;
	private final Node thisNode;
	private final byte[] serverId;
	private final SynchronousQueue<Message> outputMessageQueue = new SynchronousQueue<Message>();
	private TServer tServer;
	private TTransportException tTransportException = null;
	private boolean running = false;
	private static final String ERROR_MSG_LIFE_TIME_EXPIRED = "Messaage is terminated as lifetime expired!";
	private static final String ERROR_MSG_MAX_SEND_RETRIES_EXCEEDED = "Messaage is terminated as maximum number of retries is exceeded!";
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
		thisNode = server.getNode();
		this.serverId = thisNode.getServerId();
	}

	public void send(Message message, Node destination) {
		if (Arrays.equals(serverId, destination.serverId.array())) {
			System.out.println(this.server.getServerIdAsString() + ": just to let you know "
					+ "that forwading within same node still happens :P \n" + message);
		}
		if (message.lifetime <= 0) {
			message.setError(ERROR_MSG_LIFE_TIME_EXPIRED);
			message.setDestination(message.source);
			message.setDestinationId(message.source.serverId);
			message.setType(MessageType.ERROR);
			message.setSuccess(false);
			message.setSource(thisNode);
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
		Node prevStation = message.getStation();
		while (tries < CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES + 1) {
			tries++;
			if (tries > CONFIGURATIONS.NUM_OF_MAX_SEND_RETRIES) {
				if (!isRoutable(message)) {
					return;
				}
				message.setError(ERROR_MSG_MAX_SEND_RETRIES_EXCEEDED);
				message.setDestination(message.source);
				message.setDestinationId(message.source.serverId);
				message.setType(MessageType.ERROR);
				message.setSuccess(false);
				message.setSource(thisNode);
				tries = 0;
			}
			message.setStation(thisNode);
			Connection connection = this.server.getConnectionPool().getConnection(message.destination);
			int result = -1;
			try {
				synchronized (connection) {
					connection.open();
					result = connection.getClient().transfer(message);
				}
				if (result == SUCCESS) {
					return;
				}
			} catch (TException ex) {
				if (connection.getNumOfOpenTries() >= CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
					connection.failedPermenently();
					boolean updated = server.getRoutingHandler().updateTable(message.destination, false);
					if (updated) {
						Logger.log(thisNode, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 1,
								"Node " + ByteArrays.idToReadableString(message.destination)
								+ " @ "
								+ message.destination.address + " is removed from the routing table!");
						server.getRoutingHandler().triggerOptimize();
					}
					if (isRoutable(message)) {
						message.setStation(prevStation);
						if (!this.server.getMessageHandler().queueMessage(message)) {
							throw new RuntimeException();
						}
					}
					return;
				}
				connection.failed();
				tries--;
			}
		}
		throw new RuntimeException("This is unacceptable :P");
	}

	public int directSend(Message message) throws TException {
		message.setStation(thisNode);
		Connection connection = this.server.getConnectionPool().getConnection(message.destination);
		synchronized (connection) {
			connection.open();
			return connection.getClient().transfer(message);
		}
	}

	public synchronized void startServer() throws TTransportException, InterruptedException {
		if (tServer == null || !tServer.isServing()) {
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						DHTServer.Processor processor = new DHTServer.Processor(new ThriftServerImpl(server));
						int localPortId = Integer.parseInt(thisNode.getAddress().split(":")[1]);
						TServerTransport serverTransport = new TServerSocket(localPortId);
						tServer = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));
						Logger.log(thisNode, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
								"Starting the server on port " + localPortId);
						tServer.serve();
						Logger.log(thisNode, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
								"Server is shutting down...");
					} catch (TTransportException ex) {
						tTransportException = ex;
					}
				}
			});
			thread.start();
			while (tTransportException == null && (tServer == null || !tServer.isServing())) {
				Thread.sleep(100);
			}
			if (tTransportException != null) {
				throw tTransportException;
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
