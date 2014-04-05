/************************************************************************************

 * Sinchana Distributed Hash table 

 * Copyright (C) 2012 Sinchana DHT - Department of Computer Science &               
 * Engineering, University of Moratuwa, Sri Lanka. Permission is hereby 
 * granted, free of charge, to any person obtaining a copy of this 
 * software and associated documentation files of Sinchana DHT, to deal 
 * in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.

 * Neither the name of University of Moratuwa, Department of Computer Science 
 * & Engineering nor the names of its contributors may be used to endorse or 
 * promote products derived from this software without specific prior written 
 * permission.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
 * SOFTWARE.                                                                    
 ************************************************************************************/
package sinchana;

import java.util.concurrent.SynchronousQueue;
import org.apache.thrift.TException;

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

	public static final int ACCEPT_ERROR = 1;
	public static final int SUCCESS = 2;
	private final SinchanaServer server;
	private final Node thisNode;
	private final SynchronousQueue<Message> outputMessageQueue = new SynchronousQueue<Message>();
	private TServer tServer;
	private TTransportException tTransportException = null;
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
	IOHandler(SinchanaServer svr) {
		this.server = svr;
		thisNode = server.getNode();
	}

	public void send(Message message, Node destination) {
		if (message.lifetime <= 0) {
			if (!message.responseExpected) {
				return;
			}
			message.setError(SinchanaDHT.ERROR_MSG_LIFE_TIME_EXPIRED);
			message.setDestination(message.source);
			message.setDestinationId(message.source.serverId);
			message.setType(MessageType.ERROR);
			message.setSuccess(false);
			message.setLifetime(1);
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
		while (tries < SinchanaDHT.NUM_OF_MAX_SEND_RETRIES + 1) {
			tries++;
			if (tries > SinchanaDHT.NUM_OF_MAX_SEND_RETRIES) {
				if (!isRoutable(message) || !message.responseExpected) {
					return;
				}
				message.setError(SinchanaDHT.ERROR_MSG_MAX_SEND_RETRIES_EXCEEDED);
				message.setDestination(message.source);
				message.setDestinationId(message.source.serverId);
				message.setType(MessageType.ERROR);
				message.setSuccess(false);
				message.setSource(thisNode);
				message.setLifetime(1);
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
				if (connection.getNumOfOpenTries() >= SinchanaDHT.NUM_OF_MAX_CONNECT_RETRIES) {
					connection.failedPermenently();
					boolean updated = server.getRoutingHandler().updateTable(message.destination, false);
					if (updated) {
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

	int directSend(Message message) throws TException {
		message.setStation(thisNode);
		Connection connection = this.server.getConnectionPool().getConnection(message.destination);
		synchronized (connection) {
			connection.open();
			return connection.getClient().transfer(message);
		}
	}

	synchronized void startServer() throws TTransportException, InterruptedException {
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
						Logger.log(thisNode, Logger.LogLevel.LEVEL_INFO, Logger.LogClass.CLASS_THRIFT_SERVER, 1,
								"Starting the server on port " + localPortId);
						tServer.serve();
						Logger.log(thisNode, Logger.LogLevel.LEVEL_INFO, Logger.LogClass.CLASS_THRIFT_SERVER, 1,
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
		for (int i = 0; i < SinchanaDHT.NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS; i++) {
			new Thread(outputMessageQueueProcessor).start();
		}
	}

	void stopServer() {
		running = false;
		if (tServer != null && tServer.isServing()) {
			tServer.stop();
		}
		this.server.getConnectionPool().closeAllConnections();
	}

	boolean isRunning() {
		return running;
	}
}
