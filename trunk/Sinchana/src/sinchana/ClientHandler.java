/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Arrays;
import sinchana.dataStore.SinchanaDataHandler;
import sinchana.service.SinchanaServiceHandler;
import sinchana.service.SinchanaServiceInterface;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.util.logging.Logger;
import sinchana.util.messagequeue.MessageQueue;
import sinchana.util.tools.Hash;

/**
 *
 * @author Hiru
 */
public class ClientHandler {

	private SinchanaServer server;
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
						clientData.success = message.isSuccess();
						clientData.lock.release();
					} else {
						((SinchanaServiceHandler) clientData.sinchanaCallBackHandler).serviceResponse(
								Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - CONFIGURATIONS.SERVICE_TAG.length),
								message.isSuccess(), message.getData());
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
							((SinchanaServiceHandler) clientData.sinchanaCallBackHandler).serviceFound(
									Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - CONFIGURATIONS.SERVICE_TAG.length),
									message.isSuccess(), message.getData());
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
							((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isPublished(
									Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - CONFIGURATIONS.SERVICE_TAG.length),
									message.success);
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
								((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isRemoved(
										Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - CONFIGURATIONS.SERVICE_TAG.length),
										success);
							} else {
								((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isRemoved(
										Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - CONFIGURATIONS.SERVICE_TAG.length),
										false);
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

	public ClientData addRequest(byte[] key, byte[] data, MessageType type, SinchanaCallBackHandler scbh, boolean waiting) {
		ClientData clientData = null;
		long requestId = -1;
		Message message = new Message(type, this.server, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
		switch (message.type) {
			case REQUEST:
				message.setDestinationId(key);
				message.setKey(key);
				break;
			case GET_SERVICE:
				message.setDestinationId(Arrays.copyOf(key, 20));
				message.setKey(Arrays.copyOfRange(key, 20, key.length));
				break;
			case GET_DATA:
			case STORE_DATA:
			case DELETE_DATA:
				message.setDestinationId(Hash.generateId(new String(key)));
				message.setKey(key);
				break;
		}
		message.setData(data);
		message.setStation(this.server);
		if (scbh != null || waiting) {
			requestId = System.currentTimeMillis();
			clientData = new ClientData();
			clientData.time = requestId;
			clientData.dataKey = key;
			clientData.waiting = waiting;
			clientData.sinchanaCallBackHandler = scbh;
			while (clientsMap.putIfAbsent(requestId, clientData) != null) {
				requestId++;
			}
			message.setResponseExpected(true);
			message.setId(requestId);
		} else {
			message.setResponseExpected(false);
		}
		if (server.getMessageHandler().queueMessage(message, MessageQueue.PRIORITY_LOW)) {
			if (waiting) {
				try {
					clientData.lock.acquire();
				} catch (InterruptedException ex) {
				} finally {
					clientsMap.remove(requestId);
				}
			}
		} else {
			clientsMap.remove(requestId);
			Logger.log(this.server, Logger.LEVEL_WARNING, Logger.CLASS_CLIENT_HANDLER, 0,
					"Message is unacceptable 'cos buffer is full! " + message);
		}
		return clientData;
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
