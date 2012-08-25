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

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import sinchana.dataStore.SinchanaDataStoreInterface;
import sinchana.dataStore.SinchanaDataCallback;
import sinchana.dataStore.SinchanaDataStoreImpl;
import sinchana.exceptions.SinchanaInterruptedException;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.service.SinchanaServiceStore;
import sinchana.service.SinchanaServiceCallback;
import sinchana.service.SinchanaServiceInterface;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import sinchana.exceptions.SinchanaJoinException;
import sinchana.chord.ChordTable;
import sinchana.tapastry.TapestryTable;
import sinchana.pastry.PastryTable;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.tools.ByteArrays;
import sinchana.util.tools.Hash;

/**
 *
 * @author S.A.H.S.Subasinghe
 */
public class SinchanaServer {

	/**
	 * Size of the Sinchana ring which is equal to 2^160.
	 */
	public static final BigInteger GRID_SIZE = new BigInteger("2", CONFIGURATIONS.NUMBER_BASE).pow(160);
	private static final String ERROR_MSG_JOIN_FAILED = "Join failed. Maximum number of retries exceeded!";
	private final IOHandler iOHandler;
	private final RoutingHandler routingHandler;
	private final MessageHandler messageHandler;
	private final ConnectionPool connectionPool;
	private final ClientHandler clientHandler;
	private final SinchanaServiceStore sinchanaServiceStore;
	private final BigInteger serverIdAsBigInt;
	private final Node node;
	private final String serverIdAsString;
	private final Semaphore joinLock = new Semaphore(0);
	private boolean joined = false;
	private SinchanaRequestCallback sinchanaRequestCallback = null;
	private SinchanaTestInterface sinchanaTestInterface = null;
	private SinchanaDataStoreInterface sinchanaDataStoreInterface = new SinchanaDataStoreImpl(this);

	/**
	 * 
	 * @param localPortId 
	 * @throws UnknownHostException
	 */
	public SinchanaServer(int localPortId) throws UnknownHostException {
		String localAddress = InetAddress.getLocalHost().getHostAddress() + ":" + localPortId;
		this.node = new Node(ByteBuffer.wrap(Hash.generateId(localAddress)), localAddress);
		this.serverIdAsBigInt = new BigInteger(1, this.node.serverId.array());
		this.serverIdAsString = ByteArrays.idToReadableString(node.serverId);
		this.iOHandler = new IOHandler(this);
		this.messageHandler = new MessageHandler(this);
		this.connectionPool = new ConnectionPool(this);
		this.clientHandler = new ClientHandler(this);
		this.sinchanaServiceStore = new SinchanaServiceStore();
//		this.routingHandler = new ChordTable(this);
//		this.routingHandler = new TapestryTable(this);
		this.routingHandler = new PastryTable(this);
	}

	/**
	 * Class constructor.
	 * @param localAddress 
	 */
	public SinchanaServer(String localAddress) {
		this.node = new Node(ByteBuffer.wrap(Hash.generateId(localAddress)), localAddress);
		this.serverIdAsBigInt = new BigInteger(1, this.node.serverId.array());
		this.serverIdAsString = ByteArrays.idToReadableString(node.serverId);
		this.iOHandler = new IOHandler(this);
		this.messageHandler = new MessageHandler(this);
		this.connectionPool = new ConnectionPool(this);
		this.clientHandler = new ClientHandler(this);
		this.sinchanaServiceStore = new SinchanaServiceStore();
//		this.routingHandler = new ChordTable(this);
//		this.routingHandler = new TapestryTable(this);
		this.routingHandler = new PastryTable(this);
	}

	/**
	 * 
	 * @throws TTransportException
	 * @throws InterruptedException
	 */
	public void startServer() throws TTransportException, InterruptedException {
		this.routingHandler.init();
		this.iOHandler.startServer();
	}

	/**
	 * 
	 */
	public void stopServer() {
		iOHandler.stopServer();
	}

	/**
	 * 
	 * @return
	 */
	public boolean isRunning() {
		return iOHandler.isRunning();
	}

	/**
	 * 
	 * @param remoteNodeAddress
	 * @throws TException
	 * @throws InterruptedException
	 */
	public void join(String remoteNodeAddress) throws TException, InterruptedException {
		Message msg = new Message(MessageType.JOIN, this.node, CONFIGURATIONS.JOIN_MESSAGE_LIFETIME);
		if (remoteNodeAddress != null && !remoteNodeAddress.equals(this.node.address)) {
			Node remoteNode = new Node(ByteBuffer.wrap(Hash.generateId(remoteNodeAddress)), remoteNodeAddress);
			msg.setDestination(remoteNode);
			int joinAttempt = 0;
			while (!joined) {
				if (++joinAttempt > CONFIGURATIONS.MAX_JOIN_RETRIES) {
					throw new SinchanaJoinException(ERROR_MSG_JOIN_FAILED);
				}
				System.out.println(this.serverIdAsString + ": Attempt " + joinAttempt + ": Connecting to " + remoteNodeAddress);
				this.iOHandler.directSend(msg);
				joinLock.tryAcquire(CONFIGURATIONS.JOIN_RETRY_TIME_OUT, TimeUnit.SECONDS);
			}
		} else {
			msg.setStation(this.node);
			msg.setSuccess(true);
			messageHandler.queueMessage(msg);
		}
	}

	/**
	 * 
	 */
	public void join() {
		Message msg = new Message(MessageType.JOIN, this.node, CONFIGURATIONS.JOIN_MESSAGE_LIFETIME);
		msg.setStation(this.node);
		msg.setSuccess(true);
		messageHandler.queueMessage(msg);
	}

	void setJoined(boolean joined, String status) {
		if (joined) {
			this.joined = joined;
			joinLock.release();
		} else {
			System.out.println(status);
		}
	}

	/**
	 * 
	 * @return
	 */
	public boolean isJoined() {
		return joined;
	}

	/**
	 * 
	 */
	public void printTableInfo() {
		routingHandler.printInfo();
	}

	/**
	 * 
	 * @param src
	 */
	public void registerSinchanaRequestCallback(SinchanaRequestCallback src) {
		this.sinchanaRequestCallback = src;
	}

	/**
	 * 
	 * @param sinchanaTestInterface
	 */
	public void registerSinchanaTestInterface(SinchanaTestInterface sinchanaTestInterface) {
		this.sinchanaTestInterface = sinchanaTestInterface;
	}

	/**
	 * 
	 * @param sdsi
	 */
	public void registerSinchanaStoreInterface(SinchanaDataStoreInterface sdsi) {
		this.sinchanaDataStoreInterface = sdsi;
	}

	MessageHandler getMessageHandler() {
		return messageHandler;
	}

	/**
	 * 
	 * @return
	 */
	public IOHandler getIOHandler() {
		return iOHandler;
	}

	RoutingHandler getRoutingHandler() {
		return routingHandler;
	}

	/**
	 * 
	 * @return
	 */
	public ConnectionPool getConnectionPool() {
		return connectionPool;
	}

	ClientHandler getClientHandler() {
		return clientHandler;
	}

	SinchanaServiceStore getSinchanaServiceStore() {
		return sinchanaServiceStore;
	}

	SinchanaDataStoreInterface getSinchanaDataStoreInterface() {
		return sinchanaDataStoreInterface;
	}

	/**
	 * 
	 * @return
	 */
	public Node getNode() {
		return node;
	}

	/**
	 * 
	 * @return
	 */
	public BigInteger getServerIdAsBigInt() {
		return serverIdAsBigInt;
	}

	/**
	 * 
	 * @return
	 */
	public String getServerIdAsString() {
		return serverIdAsString;
	}

	SinchanaTestInterface getSinchanaTestInterface() {
		return sinchanaTestInterface;
	}

	/**
	 * 
	 * @return
	 */
	public SinchanaRequestCallback getSinchanaRequestCallback() {
		return sinchanaRequestCallback;
	}	

	/**
	 * 
	 */
	public void testRing() {
		Message message = new Message(MessageType.TEST_RING, this.node, 1024);
		message.setStation(this.node);
		this.getMessageHandler().queueMessage(message);
	}

	/**
	 * 
	 * @param destination
	 * @param message
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] sendRequest(byte[] destination, byte[] message) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(destination, message, MessageType.REQUEST, -1, null).data;
	}

	/**
	 * 
	 * @param destination
	 * @param message
	 * @param timeOut
	 * @param timeUnit
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] sendRequest(byte[] destination, byte[] message, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(destination, message, MessageType.REQUEST, timeOut, timeUnit).data;
	}

	/**
	 * 
	 * @param destination
	 * @param message
	 * @param callBack
	 * @throws InterruptedException
	 */
	public void sendRequest(byte[] destination, byte[] message, SinchanaResponseCallback callBack) throws InterruptedException {
		this.clientHandler.addRequest(destination, message, MessageType.REQUEST, callBack);
	}

	/**
	 * 
	 * @param key
	 * @param data
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public boolean storeData(byte[] key, byte[] data) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, -1, null).success;
	}

	/**
	 * 
	 * @param key
	 * @param data
	 * @param timeOut
	 * @param timeUnit
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public boolean storeData(byte[] key, byte[] data, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, timeOut, timeUnit).success;
	}

	/**
	 * 
	 * @param key
	 * @param data
	 * @param callBack
	 * @throws InterruptedException
	 */
	public void storeData(byte[] key, byte[] data, SinchanaDataCallback callBack) throws InterruptedException {
		this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, callBack);
	}

	/**
	 * 
	 * @param key
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] getData(byte[] key) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.GET_DATA, -1, null).data;
	}

	/**
	 * 
	 * @param key
	 * @param timeOut
	 * @param timeUnit
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] getData(byte[] key, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.GET_DATA, timeOut, timeUnit).data;
	}

	/**
	 * 
	 * @param key
	 * @param callBack
	 * @throws InterruptedException
	 */
	public void getData(byte[] key, SinchanaDataCallback callBack) throws InterruptedException {
		this.clientHandler.addRequest(key, null, MessageType.GET_DATA, callBack);
	}

	/**
	 * 
	 * @param key
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public boolean deleteData(byte[] key) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, -1, null).success;
	}

	/**
	 * 
	 * @param key
	 * @param timeOut
	 * @param timeUnit
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public boolean deleteData(byte[] key, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, timeOut, timeUnit).success;
	}

	/**
	 * 
	 * @param key
	 * @param callBack
	 * @throws InterruptedException
	 */
	public void deleteData(byte[] key, SinchanaDataCallback callBack) throws InterruptedException {
		this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, callBack);
	}

	/**
	 * 
	 * @param reference
	 * @param data
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] invokeService(byte[] reference, byte[] data) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, -1, null).data;
	}

	/**
	 * 
	 * @param reference
	 * @param data
	 * @param timeOut
	 * @param timeUnit
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] invokeService(byte[] reference, byte[] data, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, timeOut, timeUnit).data;
	}

	/**
	 * 
	 * @param reference
	 * @param data
	 * @param callBack
	 * @throws InterruptedException
	 */
	public void invokeService(byte[] reference, byte[] data, SinchanaServiceCallback callBack) throws InterruptedException {
		this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, callBack);
	}

	/**
	 * 
	 * @param key
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] discoverService(byte[] key) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		return this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, -1, null).data;
	}

	/**
	 * 
	 * @param key
	 * @param timeOut
	 * @param timeUnit
	 * @return
	 * @throws InterruptedException
	 * @throws SinchanaTimeOutException
	 * @throws SinchanaInterruptedException
	 */
	public byte[] discoverService(byte[] key, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		return this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, timeOut, timeUnit).data;
	}

	/**
	 * 
	 * @param key
	 * @param callBack
	 * @throws InterruptedException
	 */
	public void discoverService(byte[] key, SinchanaServiceCallback callBack) throws InterruptedException {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, callBack);
	}

	/**
	 * 
	 * @param key
	 * @param ssi
	 * @throws InterruptedException
	 */
	public void publishService(byte[] key, SinchanaServiceInterface ssi) throws InterruptedException {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		byte[] formattedReference = ByteArrays.arrayConcat(this.node.serverId.array(), formattedKey);
		boolean success = this.sinchanaServiceStore.publishService(formattedKey, ssi);
		if (success) {
			this.clientHandler.addRequest(formattedKey, formattedReference, MessageType.STORE_DATA, ssi);
		} else {
			ssi.isPublished(key, false);
		}
	}

	/**
	 * 
	 * @param key
	 * @param ssi
	 * @throws InterruptedException
	 */
	public void removeService(byte[] key, SinchanaServiceInterface ssi) throws InterruptedException {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		this.clientHandler.addRequest(formattedKey, null, MessageType.DELETE_DATA, ssi);
	}
}
