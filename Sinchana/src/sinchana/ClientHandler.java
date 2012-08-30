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

import java.util.Arrays;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import sinchana.dataStore.SinchanaDataCallback;
import sinchana.service.SinchanaServiceCallback;
import sinchana.service.SinchanaServiceInterface;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.tools.Hash;

/**
 * This class is to keep client requests and to map them with the responses
 * @author S.A.H.S.Subasinghe <hirantha.subasinghe@gmail.com>
 */
public class ClientHandler {

	/*Sinchana Server instance*/
	private final SinchanaServer server;
	/*Node info*/
	private final Node thisNode;
	/*Client request-id map*/
	private final ConcurrentHashMap<Long, ClientData> clientsMap = new ConcurrentHashMap<Long, ClientData>();
	private final Timer timer = new Timer();

	ClientHandler(SinchanaServer svr) {
		this.server = svr;
		this.thisNode = server.getNode();
		/*Schedule a task to the timer with 1 second period. At each second, request-id map
		is checked and old but not resolved requests are discarded.*/
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				Collection<ClientData> values = clientsMap.values();
				long currentTimeMillis = System.currentTimeMillis();
				for (ClientData cd : values) {
					if (cd.time + SinchanaDHT.ASYNCHRONOUS_REQUEST_TIME_OUT * 1000 < currentTimeMillis) {
						ClientData clientData = clientsMap.remove(cd.key);
						if (clientData != null) {
							clientData.resolved = false;
							if (clientData.waiting) {
								clientData.success = false;
								clientData.error = SinchanaDHT.ERROR_MSG_TIMED_OUT;
								clientData.lock.release();
							} else {
								clientData.sinchanaCallBackHandler.error(SinchanaDHT.ERROR_MSG_TIMED_OUT);
							}
						}
					}
				}
			}
		}, 1000, 1000);
	}

	/*
	 * This method maps the responses' ids with the requests' ids.
	 */
	void setResponse(Message message) {
		ClientData clientData = clientsMap.remove(message.getId());
		if (clientData != null) {
			/*Updating test data*/
			if (server.getSinchanaTestInterface() != null) {
				server.getSinchanaTestInterface().incRequestCount(message.lifetime, message.routedViaPredecessors);
			}
			clientData.resolved = true;
			if (clientData.waiting) {
				/*If the request is an asynchronous one*/
				clientData.success = message.isSuccess();
				clientData.data = message.isSetData() ? message.getData() : null;
				clientData.error = message.isSetError() ? message.getError() : null;
				/*Release the thread by releasing the semaphore*/
				clientData.lock.release();
			} else {
				/*If the request is synchronous*/
				switch (message.type) {
					case RESPONSE_SERVICE:
						((SinchanaServiceCallback) clientData.sinchanaCallBackHandler).serviceResponse(
								Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - SinchanaDHT.SERVICE_TAG.length),
								message.isSuccess(), message.getData());
						break;
					case RESPONSE_DATA:
						/*Data store request can be bother data requests and service requests. 
						 * They are separated by looking at the type of the callback*/
						if (clientData.sinchanaCallBackHandler instanceof SinchanaDataCallback) {
							((SinchanaDataCallback) clientData.sinchanaCallBackHandler).response(clientData.dataKey, message.getData());
						} else if (clientData.sinchanaCallBackHandler instanceof SinchanaServiceCallback) {
							((SinchanaServiceCallback) clientData.sinchanaCallBackHandler).serviceFound(
									Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - SinchanaDHT.SERVICE_TAG.length),
									message.isSuccess(), message.getData());
						}
						break;
					case ACKNOWLEDGE_DATA_STORE:
						if (clientData.sinchanaCallBackHandler instanceof SinchanaDataCallback) {
							((SinchanaDataCallback) clientData.sinchanaCallBackHandler).isStored(clientData.dataKey, message.success);
						} else if (clientData.sinchanaCallBackHandler instanceof SinchanaServiceInterface) {
							((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isPublished(
									Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - SinchanaDHT.SERVICE_TAG.length),
									message.success);
						}
						break;
					case ACKNOWLEDGE_DATA_REMOVE:
						if (clientData.sinchanaCallBackHandler instanceof SinchanaDataCallback) {
							((SinchanaDataCallback) clientData.sinchanaCallBackHandler).isRemoved(clientData.dataKey, message.success);
						} else if (clientData.sinchanaCallBackHandler instanceof SinchanaServiceInterface) {
							/*Service removel should be followed by local service remove*/
							if (message.success) {
								boolean success = this.server.getSinchanaServiceStore().removeService(clientData.dataKey);
								((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isRemoved(
										Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - SinchanaDHT.SERVICE_TAG.length),
										success);
							} else {
								((SinchanaServiceInterface) clientData.sinchanaCallBackHandler).isRemoved(
										Arrays.copyOf(clientData.dataKey, clientData.dataKey.length - SinchanaDHT.SERVICE_TAG.length),
										false);
							}
						}
						break;
					case RESPONSE:
						((SinchanaResponseCallback) clientData.sinchanaCallBackHandler).response(message.getData());
						break;
					case ERROR:
						((SinchanaCallBack) clientData.sinchanaCallBackHandler).error(message.getError());
						break;
				}
			}
		}
	}

	/*
	 * Adds a synchronous request
	 */
	ClientData addRequest(byte[] key, byte[] data, MessageType type, long timeOut, TimeUnit timeUnit) throws SinchanaTimeOutException, InterruptedException {
		long requestId = -1;
		/*Obtains the message*/
		Message message = generateMessage(type, key, data);
		requestId = System.currentTimeMillis();
		/*creating a client data object*/
		ClientData clientData = new ClientData();
		clientData.time = requestId;
		clientData.dataKey = key;
		clientData.waiting = true;
		clientData.sinchanaCallBackHandler = null;
		/*Checks availability and adds to the request-id map*/
		while (clientsMap.putIfAbsent(requestId, clientData) != null) {
			requestId++;
		}
		clientData.key = requestId;
		message.setResponseExpected(true);
		message.setId(requestId);
		try {
			/*Adding request to the message queue*/
			server.getMessageHandler().addRequest(message);
			/*wait*/
			if (timeOut != -1) {
				clientData.lock.tryAcquire(timeOut, timeUnit);
			} else {
				clientData.lock.acquire();
			}
		} catch (InterruptedException ex) {
			throw ex;
		} finally {
			/*Remove the request from the request-id map*/
			clientsMap.remove(requestId);
		}
		if (!clientData.resolved) {
			throw new SinchanaTimeOutException();
		}
		return clientData;
	}

	/*
	 * Adds an asynchronous request
	 */
	void addRequest(byte[] key, byte[] data, MessageType type, SinchanaCallBack scbh) throws InterruptedException {
		long requestId = -1;
		/*Obtains the message*/
		Message message = generateMessage(type, key, data);
		/*if callback is null, that means it does not expect a response, otherwise, 
		 * it's expecting a response, so creating a client data object*/
		if (scbh != null) {
			requestId = System.currentTimeMillis();
			ClientData clientData = new ClientData();
			clientData.time = requestId;
			clientData.dataKey = key;
			clientData.waiting = false;
			clientData.sinchanaCallBackHandler = scbh;
			/*Checks availability and adds to the request-id map*/
			while (clientsMap.putIfAbsent(requestId, clientData) != null) {
				requestId++;
			}
			clientData.key = requestId;
			message.setResponseExpected(true);
			message.setId(requestId);
		} else {
			message.setResponseExpected(false);
		}
		/*Adding request to the message queue*/
		server.getMessageHandler().addRequest(message);
	}

	/*
	 * Generates a message for the request.
	 */
	private Message generateMessage(MessageType type, byte[] key, byte[] data) {
		Message message = new Message(type, thisNode, SinchanaDHT.REQUEST_MESSAGE_LIFETIME);
		switch (message.type) {
			case REQUEST:
				/*key is set as the destination id and target key*/
				message.setDestinationId(key);
				message.setKey(key);
				break;
			case GET_SERVICE:
				/*key and destination id is extracted from the reference key*/
				message.setDestinationId(Arrays.copyOf(key, 20));
				message.setKey(Arrays.copyOfRange(key, 20, key.length));
				break;
			case GET_DATA:
			case STORE_DATA:
			case DELETE_DATA:
				/*the hash value is used as the destination id*/
				message.setDestinationId(Hash.generateId(new String(key)));
				message.setKey(key);
				break;
		}
		if (data != null) {
			message.setData(data);
		}
		message.setStation(thisNode);
		return message;
	}

	/*
	 * This inner class is used to store iformation about requests.
	 */
	class ClientData {

		long key;
		final Semaphore lock = new Semaphore(0);
		byte[] dataKey;
		byte[] data;
		byte[] error;
		boolean valid = true, success = false, waiting = false, resolved = false;
		long time;
		SinchanaCallBack sinchanaCallBackHandler;
	}
}
