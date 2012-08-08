/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.DataObject;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.ServiceObject;
import sinchana.thrift.SinchanaClient;
import sinchana.util.logging.Logger;

/**
 *
 * @author Hiru
 */
public class ClientHandler {

	private Server server;
	private TServer tServer;
	private final ConcurrentHashMap<Long, ClientData> clientsMap = new ConcurrentHashMap<Long, ClientData>();

	public ClientHandler(Server svr) {
		this.server = svr;
	}

	public void setResponse(Message message) {
		ClientData clientData = clientsMap.get(message.getId());
		switch (message.type) {
			case RESPONSE_SERVICE:
				clientData.services = message.getServiceSet();
				break;
			case RESPONSE_DATA:
				clientData.data = message.getDataSet();
				break;
			case ACKNOWLEDGE_SERVICE_PUBLISH:
			case ACKNOWLEDGE_DATA_STORE:
				clientData.success = true;
				break;
			case ACKNOWLEDGE_DATA_REMOVE:
			case ACKNOWLEDGE_SERVICE_REMOVE:
				clientData.success = false;
				break;
			case RESPONSE:
				clientData.message = message.getMessage();
				break;
			case ERROR:
				clientData.message = null;
				break;
		}
		clientData.lock.release();
	}

	public void startClientServer(final int portId) {
		if (tServer == null || !tServer.isServing()) {
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						SinchanaClient.Processor processor = new SinchanaClient.Processor(new SinchanaClient.Iface() {

							@Override
							public boolean publishService(ServiceObject services) throws TException {
								long key = Calendar.getInstance().getTimeInMillis();
								try {
									ClientData clientData = new ClientData();
									clientData.time = key;
									while (clientsMap.put(key, clientData) != null) {
										key++;
									}
									Set<ServiceObject> serviceObjects = new HashSet<ServiceObject>();
									serviceObjects.add(services);
									Message message = new Message();
									message.setType(MessageType.PUBLISH_SERVICE);
									message.setTargetKey(services.sourceID);
									message.setServiceSet(serviceObjects);
									message.setId(key);
									server.send(message);
									clientData.lock.acquire();
									return clientData.success;
								} catch (InterruptedException ex) {
									return false;
								} finally {
									clientsMap.remove(key);
								}
							}

							@Override
							public boolean removeService(String serviceKey) throws TException {
								long key = Calendar.getInstance().getTimeInMillis();
								try {
									ClientData clientData = new ClientData();
									clientData.time = key;
									while (clientsMap.put(key, clientData) != null) {
										key++;
									}
									Message message = new Message();
									message.setType(MessageType.REMOVE_SERVICE);
									message.setTargetKey(serviceKey);
									message.setId(key);
									server.send(message);
									clientData.lock.acquire();
									return clientData.success;
								} catch (InterruptedException ex) {
									return false;
								} finally {
									clientsMap.remove(key);
								}
							}

							@Override
							public Set<ServiceObject> getService(String serviceKey) throws TException {
								long key = Calendar.getInstance().getTimeInMillis();
								try {
									ClientData clientData = new ClientData();
									clientData.time = key;
									while (clientsMap.put(key, clientData) != null) {
										key++;
									}
									Message message = new Message();
									message.setType(MessageType.GET_SERVICE);
									message.setTargetKey(serviceKey);
									message.setId(key);
									server.send(message);
									clientData.lock.acquire();
									return clientData.services;
								} catch (InterruptedException ex) {
									return null;
								} finally {
									clientsMap.remove(key);
								}
							}

							@Override
							public boolean publishData(DataObject data) throws TException {
								long key = Calendar.getInstance().getTimeInMillis();
								try {
									ClientData clientData = new ClientData();
									clientData.time = key;
									while (clientsMap.put(key, clientData) != null) {
										key++;
									}
									Set<DataObject> dataObjects = new HashSet<DataObject>();
									dataObjects.add(data);
									Message message = new Message();
									message.setType(MessageType.STORE_DATA);
									message.setTargetKey(data.sourceID);
									message.setDataSet(dataObjects);
									message.setId(key);
									server.send(message);
									clientData.lock.acquire();
									return clientData.success;
								} catch (InterruptedException ex) {
									return false;
								} finally {
									clientsMap.remove(key);
								}
							}

							@Override
							public boolean removeData(String dataKey) throws TException {
								long key = Calendar.getInstance().getTimeInMillis();
								try {
									ClientData clientData = new ClientData();
									clientData.time = key;
									while (clientsMap.put(key, clientData) != null) {
										key++;
									}
									Message message = new Message();
									message.setType(MessageType.DELETE_DATA);
									message.setTargetKey(dataKey);
									message.setId(key);
									server.send(message);
									clientData.lock.acquire();
									return clientData.success;
								} catch (InterruptedException ex) {
									return false;
								} finally {
									clientsMap.remove(key);
								}
							}

							@Override
							public Set<DataObject> getData(String dataKey) throws TException {
								long key = Calendar.getInstance().getTimeInMillis();
								try {
									ClientData clientData = new ClientData();
									clientData.time = key;
									while (clientsMap.put(key, clientData) != null) {
										key++;
									}
									Message message = new Message();
									message.setType(MessageType.GET_DATA);
									message.setTargetKey(dataKey);
									message.setId(key);
									server.send(message);
									clientData.lock.acquire();
									return clientData.data;
								} catch (InterruptedException ex) {
									return null;
								} finally {
									clientsMap.remove(key);
								}
							}

							@Override
							public String request(String destination) throws TException {
								long key = Calendar.getInstance().getTimeInMillis();
								try {
									ClientData clientData = new ClientData();
									clientData.time = key;
									while (clientsMap.put(key, clientData) != null) {
										key++;
									}
									Message message = new Message();
									message.setType(MessageType.REQUEST);
									message.setTargetKey(destination);
									message.setId(key);
									server.send(message);
									clientData.lock.acquire();
									return clientData.message;
								} catch (InterruptedException ex) {
									return null;
								} finally {
									clientsMap.remove(key);
								}
							}
						});
						TServerTransport serverTransport = new TServerSocket(portId);
						tServer = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));
						Logger.log(server.serverId, Logger.LEVEL_INFO, Logger.CLASS_CLIENT_HANDLER, 0,
								"Starting the server on port " + portId);
						tServer.serve();
						Logger.log(server.serverId, Logger.LEVEL_INFO, Logger.CLASS_CLIENT_HANDLER, 1,
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

	private class ClientData {

		final Semaphore lock = new Semaphore(0);
		String message;
		Set<ServiceObject> services;
		Set<DataObject> data;
		boolean success;
		long time;
	}
}
