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

import sinchana.dataStore.SinchanaDataStoreInterface;
import sinchana.dataStore.SinchanaDataHandler;
import sinchana.dataStore.SinchanaDataStoreImpl;
import sinchana.exceptions.SinchanaInterruptedException;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.service.SinchanaServiceStore;
import sinchana.service.SinchanaServiceHandler;
import sinchana.service.SinchanaServiceInterface;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import sinchana.chord.ChordTable;
import sinchana.connection.ConnectionPool;
import sinchana.pastry.PastryTable;
import sinchana.tapastry.TapestryTable;
import sinchana.test.TesterController;
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

	public static final BigInteger GRID_SIZE = new BigInteger("2", 16).pow(160);
	private final IOHandler iOHandler = new IOHandler(this);
//	private final RoutingHandler routingHandler = new ChordTable(this);
//	private final RoutingHandler routingHandler = new TapestryTable(this);
	private final RoutingHandler routingHandler = new PastryTable(this);
	private final MessageHandler messageHandler = new MessageHandler(this);
	private final ConnectionPool connectionPool = new ConnectionPool(this);
	private final ClientHandler clientHandler = new ClientHandler(this);
	private final SinchanaServiceStore sinchanaServiceStore = new SinchanaServiceStore();
	private final String remoteNodeAddress;
	private final BigInteger serverIdAsBigInt;
	private final Node node;
	private final String serverIdAsString;
	private boolean joined = false;
	private SinchanaRequestHandler SinchanaRequestHandler = null;
	private SinchanaTestInterface sinchanaTestInterface = null;
	private SinchanaDataStoreInterface sinchanaDataStoreInterface = new SinchanaDataStoreImpl(this);

	/**
	 * Start a new node with the given server ID and next hop.
	 * @param serverId		SinchanaServer ID. Generated using a hash function. 
	 * @param anotherNode	Another node is the network. New node first
	 * communicate with this node to discover the rest of the network.
	 * @param address		URL of the server.
	 * @param portId		Port Id number where the the server is running.
	 */
	public SinchanaServer(int localPortId) throws UnknownHostException {
		InetAddress inetAddress = InetAddress.getLocalHost();
		String localAddress = inetAddress.getHostAddress() + ":" + localPortId;
		this.node = new Node(ByteBuffer.wrap(Hash.generateId(localAddress)), localAddress);
		this.serverIdAsBigInt = new BigInteger(1, this.node.getServerId());
		this.serverIdAsString = serverIdAsBigInt.toString(CONFIGURATIONS.NUMBER_BASE).toUpperCase();
		this.remoteNodeAddress = null;
	}

	public SinchanaServer(String localAddress) {
		this.node = new Node(ByteBuffer.wrap(Hash.generateId(localAddress)), localAddress);
		this.serverIdAsBigInt = new BigInteger(1, this.node.getServerId());
		this.serverIdAsString = serverIdAsBigInt.toString(CONFIGURATIONS.NUMBER_BASE).toUpperCase();
		this.remoteNodeAddress = null;
	}

	public SinchanaServer(int localPortId, String remoteNodeAddress) throws UnknownHostException {
		InetAddress inetAddress = InetAddress.getLocalHost();
		String localAddress = inetAddress.getHostAddress() + ":" + localPortId;
		this.node = new Node(ByteBuffer.wrap(Hash.generateId(localAddress)), localAddress);
		this.serverIdAsBigInt = new BigInteger(1, this.node.getServerId());
		this.serverIdAsString = serverIdAsBigInt.toString(CONFIGURATIONS.NUMBER_BASE).toUpperCase();
		this.remoteNodeAddress = remoteNodeAddress;
	}

	public SinchanaServer(String localAddress, String remoteNodeAddress) {
		this.node = new Node(ByteBuffer.wrap(Hash.generateId(localAddress)), localAddress);
		this.serverIdAsBigInt = new BigInteger(1, this.node.getServerId());
		this.serverIdAsString = serverIdAsBigInt.toString(CONFIGURATIONS.NUMBER_BASE).toUpperCase();
		this.remoteNodeAddress = remoteNodeAddress;
	}

	/**
	 * Start the server.
	 */
	public void startServer() {
		this.routingHandler.init();
		this.iOHandler.startServer();
	}

	public boolean join() {
		Message msg = new Message(MessageType.JOIN, this.node, CONFIGURATIONS.JOIN_MESSAGE_LIFETIME);
		if (this.remoteNodeAddress != null && !this.remoteNodeAddress.equals(this.node.address)) {
			Node remoteNode = new Node(ByteBuffer.wrap(Hash.generateId(remoteNodeAddress)), remoteNodeAddress);
			int count = CONFIGURATIONS.JOIN_RETRY_TIME_OUT * 100;
			int joinAttempt = 0;
			while (!joined) {
				try {
					count++;
					if (count > CONFIGURATIONS.JOIN_RETRY_TIME_OUT * 100) {
						count = 0;
						if (++joinAttempt > CONFIGURATIONS.MAX_JOIN_RETRIES) {
							System.out.println(this + ": Join failed!");
							return false;
						}
						System.out.println(this + ": Attempt " + joinAttempt + ": Connecting to " + remoteNodeAddress);
						this.iOHandler.send(msg, remoteNode);
					}
					Thread.sleep(10);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
			}
		} else {
			msg.setStation(this.node);
			messageHandler.queueMessage(msg, true);
		}
		return true;
	}

	void setJoined(boolean joined) {
		this.joined = joined;
	}

	/**
	 * Stop the server.
	 */
	public void stopServer() {
		iOHandler.stopServer();
	}

	/**
	 * Register SinchanaRequestHandler. The callback functions in this interface 
	 * will be called when an event occurs. 
	 * @param sinchanaInterface		SinchanaRequestHandler instance.
	 */
	public void registerSinchanaRequestHandler(SinchanaRequestHandler srh) {
		this.SinchanaRequestHandler = srh;
	}

	/**
	 * Register SinchanaTestInterface. This is only for the testing purposes. 
	 * The callback functions in this interface will be called when an event occurs. 
	 * @param sinchanaTestInterface			SinchanaTestInteface instance.
	 */
	public void registerSinchanaTestInterface(SinchanaTestInterface sinchanaTestInterface) {
		this.sinchanaTestInterface = sinchanaTestInterface;
	}

	public void registerSinchanaStoreInterface(SinchanaDataStoreInterface sdsi) {
		this.sinchanaDataStoreInterface = sdsi;
	}

	/**
	 * 
	 * @return
	 */
	public MessageHandler getMessageHandler() {
		return messageHandler;
	}

	/**
	 * 
	 * @return
	 */
	public IOHandler getIOHandler() {
		return iOHandler;
	}

	/**
	 * 
	 * @return
	 */
	public RoutingHandler getRoutingHandler() {
		return routingHandler;
	}

	public ConnectionPool getConnectionPool() {
		return connectionPool;
	}

	public ClientHandler getClientHandler() {
		return clientHandler;
	}

	public SinchanaServiceStore getSinchanaServiceStore() {
		return sinchanaServiceStore;
	}

	public SinchanaDataStoreInterface getSinchanaDataStoreInterface() {
		return sinchanaDataStoreInterface;
	}

	public Node getNode() {
		return node;
	}

	public BigInteger getServerIdAsBigInt() {
		return serverIdAsBigInt;
	}

	public String getServerIdAsString() {
		return serverIdAsString;
	}

	/**
	 * 
	 * @return
	 */
	public SinchanaTestInterface getSinchanaTestInterface() {
		return sinchanaTestInterface;
	}

	public sinchana.SinchanaRequestHandler getSinchanaRequestHandler() {
		return SinchanaRequestHandler;
	}

	/**
	 * Send a message. If the message type is MessageType.GET, targetKey field should be set.
	 * @param message		Message to pass to the network.
	 */
	public void testRing() {
		Message message = new Message(MessageType.TEST_RING, this.node, 1024);
		message.setStation(this.node);
		this.getMessageHandler().queueMessage(message, true);
	}

	public byte[] request(byte[] destination, byte[] message) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(destination, message, MessageType.REQUEST, -1, null).data;
	}

	public byte[] request(byte[] destination, byte[] message, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(destination, message, MessageType.REQUEST, timeOut, timeUnit).data;
	}

	public void request(byte[] destination, byte[] message, SinchanaResponseHandler callBack) {
		this.clientHandler.addRequest(destination, message, MessageType.REQUEST, callBack);
	}

	public boolean storeData(byte[] key, byte[] data) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, -1, null).success;
	}

	public boolean storeData(byte[] key, byte[] data, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, timeOut, timeUnit).success;
	}

	public void storeData(byte[] key, byte[] data, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, callBack);
	}

	public byte[] getData(byte[] key) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.GET_DATA, -1, null).data;
	}

	public byte[] getData(byte[] key, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.GET_DATA, timeOut, timeUnit).data;
	}

	public void getData(byte[] key, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, null, MessageType.GET_DATA, callBack);
	}

	public boolean deleteData(byte[] key) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, -1, null).success;
	}

	public boolean deleteData(byte[] key, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, timeOut, timeUnit).success;
	}

	public void deleteData(byte[] key, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, callBack);
	}

	public byte[] getService(byte[] reference, byte[] data) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, -1, null).data;
	}

	public byte[] getService(byte[] reference, byte[] data, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		return this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, timeOut, timeUnit).data;
	}

	public void getService(byte[] reference, byte[] data, SinchanaServiceHandler callBack) {
		this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, callBack);
	}

	public byte[] discoverService(byte[] key) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		return this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, -1, null).data;
	}

	public byte[] discoverService(byte[] key, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException, SinchanaInterruptedException {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		return this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, timeOut, timeUnit).data;
	}

	public void discoverService(byte[] key, SinchanaServiceHandler callBack) {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, callBack);
	}

	public void publishService(byte[] key, SinchanaServiceInterface ssi) {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		byte[] formattedReference = ByteArrays.arrayConcat(this.node.getServerId(), formattedKey);
		boolean success = this.sinchanaServiceStore.publishService(formattedKey, ssi);
		if (success) {
			this.clientHandler.addRequest(formattedKey, formattedReference, MessageType.STORE_DATA, ssi);
		} else {
			ssi.isPublished(key, false);
		}
	}

	public void removeService(byte[] key, SinchanaServiceInterface ssi) {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		this.clientHandler.addRequest(formattedKey, null, MessageType.DELETE_DATA, ssi);
	}
}
