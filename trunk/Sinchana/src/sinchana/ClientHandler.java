/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.nio.ByteBuffer;
import sinchana.dataStore.SinchanaDataHandler;
import sinchana.service.SinchanaServiceHandler;
import sinchana.service.SinchanaServiceInterface;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.SinchanaClient;
import sinchana.util.logging.Logger;
import sinchana.util.tools.CommonTools;

/**
 *
 * @author Hiru
 */
public class ClientHandler {

	private SinchanaServer server;
	private TServer tServer;
	private final ConcurrentHashMap<Long, ClientData> clientsMap = new ConcurrentHashMap<Long, ClientData>();

	public ClientHandler(SinchanaServer svr) {
		this.server = svr;
	}

	public void setResponse(Message message) {
		if (clientsMap.containsKey(message.getId())) {
			ClientData clientData = clientsMap.get(message.getId());
			switch (message.type) {
				case RESPONSE_SERVICE:
					if (clientData.waiting) {
						clientData.data = message.getData();
						clientData.lock.release();
					} else {
						((SinchanaServiceHandler) clientData.sinchanaCallBackHandler).serviceResponse(clientData.dataKey, message.getData());
					}
					break;

				case RESPONSE_DATA:
					if (clientData.waiting) {
						clientData.data = message.getData();
						clientData.lock.release();
					} else {
						if (clientData.sinchanaCallBackHandler instanceof SinchanaDataHandler) {
							((SinchanaDataHandler) clientData.sinchanaCallBackHandler).response(clientData.dataKey, message.getData());
						} else if (clientData.sinchanaCallBackHandler instanceof SinchanaServiceHandler) {
							((SinchanaServiceHandler) clientData.sinchanaCallBackHandler).serviceFound(clientData.dataKey, message.getData());
						}
					}
					break;
				case ACKNOWLEDGE_DATA_STORE:
					if (clientData.waiting) {
						clientData.data = message.getData();
						clientData.lock.release();
					} else {
						if (clientData.sinchanaCallBackHandler instanceof SinchanaDataHandler) {
							((SinchanaDataHandler) clientData.sinchanaCallBackHandler).isStored(clientData.dataKey, message.success);
						} else if (clientData.sinchanaCallBackHandler instanceof SinchanaServiceInterface) {
							((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isPublished(clientData.dataKey, message.success);
						}
					}
					break;
				case ACKNOWLEDGE_DATA_REMOVE:
					if (clientData.waiting) {
						clientData.data = message.getData();
						clientData.lock.release();
					} else {
						if (clientData.sinchanaCallBackHandler instanceof SinchanaDataHandler) {
							((SinchanaDataHandler) clientData.sinchanaCallBackHandler).isRemoved(clientData.dataKey, message.success);
						} else if (clientData.sinchanaCallBackHandler instanceof SinchanaServiceInterface) {
							if (message.success) {
								boolean success = this.server.getSinchanaServiceStore().removeService(clientData.dataKey);
								((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isRemoved(clientData.dataKey, success);
							} else {
								((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isRemoved(clientData.dataKey, false);
							}
						}
					}
					break;
				case RESPONSE:
					if (clientData.waiting) {
						clientData.data = message.getData();
						clientData.lock.release();
					} else {
						((SinchanaResponseHandler) clientData.sinchanaCallBackHandler).response(message.getData());
					}
					break;
				case ERROR:
					if (clientData.waiting) {
						clientData.data = message.getData();
						clientData.lock.release();
					} else {
						((SinchanaResponseHandler) clientData.sinchanaCallBackHandler).error(message.getData());
					}
					break;
			}
		} else {
			System.out.println("ID is not in the client map: " + message.id);
		}
	}

	public ClientData addRequest(byte[] key, byte[] data, MessageType type, SinchanaCallBackHandler srh, boolean waiting, String tag) {
		ClientData clientData = null;
		long requestId = -1;
		Message message = new Message(this.server, type, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
		switch (message.type) {
			case REQUEST:
			case GET_SERVICE:
				message.setTargetKey(key);
				break;
			case GET_DATA:
			case STORE_DATA:
			case DELETE_DATA:
				message.setTargetKey(CommonTools.generateId(new String(key) + (tag != null ? tag : "")));
				break;
		}
		message.setData(data);
		message.setStation(this.server);
		if (srh != null || waiting) {
			requestId = System.currentTimeMillis();
			clientData = new ClientData();
			clientData.time = requestId;
			clientData.dataKey = key;
			clientData.waiting = waiting;
			clientData.sinchanaCallBackHandler = srh;
			while (clientsMap.putIfAbsent(requestId, clientData) != null) {
				requestId++;
			}
			message.setResponseExpected(true);
			message.setId(requestId);
		} else {
			message.setResponseExpected(false);
		}
		server.getMessageHandler().queueMessage(message);
		if (waiting) {
			try {
				clientData.lock.acquire();
			} catch (InterruptedException ex) {
			} finally {
				clientsMap.remove(requestId);
			}
		}
		return clientData;
	}

	public void startClientServer(final int portId) {
		if (tServer == null || !tServer.isServing()) {
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						SinchanaClient.Processor processor = new SinchanaClient.Processor(new SinchanaClient.Iface() {

							@Override
							public ByteBuffer discoverService(ByteBuffer serviceKey) throws TException {
								
								return ByteBuffer.wrap(addRequest(serviceKey.array(), null, 
										MessageType.GET_DATA, null, true, CONFIGURATIONS.SERVICE_TAG).data);
							}

							@Override
							public ByteBuffer getService(ByteBuffer reference, ByteBuffer data) throws TException {
								return ByteBuffer.wrap(addRequest(reference.array(), data.array(), 
										MessageType.GET_SERVICE, null, true, null).data);
							}

							@Override
							public boolean publishData(ByteBuffer dataKey, ByteBuffer data) throws TException {
								return addRequest(dataKey.array(), data.array(), MessageType.STORE_DATA, null, true, null).success;
							}

							@Override
							public boolean removeData(ByteBuffer dataKey) throws TException {
								return addRequest(dataKey.array(), null, MessageType.DELETE_DATA, null, true, null).success;
							}

							@Override
							public ByteBuffer getData(ByteBuffer dataKey) throws TException {
								return ByteBuffer.wrap(addRequest(dataKey.array(), null, MessageType.GET_DATA, null, true, null).data);
							}

							@Override
							public ByteBuffer request(ByteBuffer destination, ByteBuffer message) throws TException {
								return ByteBuffer.wrap(addRequest(destination.array(), message.array(), 
										MessageType.REQUEST, null, true, null).data);
							}
						});
						TServerTransport serverTransport = new TServerSocket(portId);
						tServer = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));
						Logger.log(server, Logger.LEVEL_INFO, Logger.CLASS_CLIENT_HANDLER, 0,
								"Starting the server on port " + portId);
						tServer.serve();
						Logger.log(server, Logger.LEVEL_INFO, Logger.CLASS_CLIENT_HANDLER, 1,
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
	}

	public class ClientData {

		final Semaphore lock = new Semaphore(0);
		byte[] dataKey; 
		byte[] data;
		boolean success, waiting = false;
		long time;
		SinchanaCallBackHandler sinchanaCallBackHandler;
	}
}
