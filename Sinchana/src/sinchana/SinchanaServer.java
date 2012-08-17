/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.dataStore.SinchanaDataStoreInterface;
import sinchana.dataStore.SinchanaDataHandler;
import sinchana.dataStore.SinchanaDataStoreImpl;
import sinchana.service.SinchanaServiceStore;
import sinchana.service.SinchanaServiceHandler;
import sinchana.service.SinchanaServiceInterface;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import sinchana.chord.ChordTable;
import sinchana.connection.ConnectionPool;
import sinchana.tapastry.TapestryTable;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.messagequeue.MessageQueue;
import sinchana.util.tools.ByteArrays;
import sinchana.util.tools.Hash;

/**
 *
 * @author S.A.H.S.Subasinghe
 */
public class SinchanaServer extends Node {

	public static final BigInteger GRID_SIZE = new BigInteger("2", 16).pow(160);
	private final PortHandler portHandler = new IOHandler(this);
	private final RoutingHandler routingHandler = new TapestryTable(this);
	private final MessageHandler messageHandler = new MessageHandler(this);
	private final ConnectionPool connectionPool = new ConnectionPool(this);
	private final ClientHandler clientHandler = new ClientHandler(this);
	private final SinchanaServiceStore sinchanaServiceStore = new SinchanaServiceStore();
	private final String remoteNodeAddress;
	private final BigInteger serverIdAsBigInt;
	private final String serverIdAsString;
	private boolean joined = false;
	private SinchanaRequestHandler SinchanaRequestHandler = null;
	private SinchanaTestInterface sinchanaTestInterface = null;
	private SinchanaDataStoreInterface sinchanaDataStoreInterface = new SinchanaDataStoreImpl();

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
		this.address = inetAddress.getHostAddress() + ":" + localPortId;
		this.setServerId(Hash.generateId(this.address));
		this.serverIdAsBigInt = new BigInteger(1, this.getServerId());
		this.serverIdAsString = ByteArrays.toReadableString(this.getServerId());
		this.remoteNodeAddress = null;
	}

	public SinchanaServer(String localAddress) {
		this.address = localAddress;
		this.setServerId(Hash.generateId(this.address));
		this.serverIdAsBigInt = new BigInteger(1, this.getServerId());
		this.serverIdAsString = ByteArrays.toReadableString(this.getServerId());
		this.remoteNodeAddress = null;
	}

	public SinchanaServer(int localPortId, String remoteNodeAddress) throws UnknownHostException {
		InetAddress inetAddress = InetAddress.getLocalHost();
		this.address = inetAddress.getHostAddress() + ":" + localPortId;
		this.setServerId(Hash.generateId(this.address));
		this.serverIdAsBigInt = new BigInteger(1, this.getServerId());
		this.serverIdAsString = ByteArrays.toReadableString(this.getServerId());
		this.remoteNodeAddress = remoteNodeAddress;
	}

	public SinchanaServer(String localAddress, String remoteNodeAddress) {
		this.address = localAddress;
		this.setServerId(Hash.generateId(this.address));
		this.serverIdAsBigInt = new BigInteger(1, this.getServerId());
		this.serverIdAsString = ByteArrays.toReadableString(this.getServerId());
		this.remoteNodeAddress = remoteNodeAddress;
	}

	/**
	 * Start the server.
	 */
	public void startServer() {
		this.routingHandler.init();
		this.portHandler.startServer();
	}

	public boolean join() {
		if (this.remoteNodeAddress != null && !this.remoteNodeAddress.equals(this.address)) {
			Message msg = new Message(MessageType.JOIN, this, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
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
						this.portHandler.send(msg, remoteNode);
					}
					Thread.sleep(10);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
			}
		} else {
			Message msg = new Message(MessageType.JOIN, this, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
			msg.setStation(this);
			messageHandler.queueMessage(msg, MessageQueue.PRIORITY_HIGH);
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
		portHandler.stopServer();
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
	public PortHandler getPortHandler() {
		return portHandler;
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
		Message message = new Message(MessageType.TEST_RING, this, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
		message.setStation(this);
		this.getMessageHandler().queueMessage(message, MessageQueue.PRIORITY_LOW);
	}

	public byte[] request(byte[] destination, byte[] message) {
		return this.clientHandler.addRequest(destination, message, MessageType.REQUEST, null, true).data;
	}

	public void request(byte[] destination, byte[] message, SinchanaResponseHandler callBack) {
		this.clientHandler.addRequest(destination, message, MessageType.REQUEST, callBack, false);
	}

	public boolean storeData(byte[] key, byte[] data) {
		return this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, null, true).success;
	}

	public void storeData(byte[] key, byte[] data, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, callBack, false);
	}

	public byte[] getData(byte[] key) {
		return this.clientHandler.addRequest(key, null, MessageType.GET_DATA, null, true).data;
	}

	public void getData(byte[] key, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, null, MessageType.GET_DATA, callBack, false);
	}

	public boolean deleteData(byte[] key) {
		return this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, null, true).success;
	}

	public void deleteData(byte[] key, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, callBack, false);
	}

	public byte[] getService(byte[] reference, byte[] data) {
		return this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, null, true).data;
	}

	public void getService(byte[] reference, byte[] data, SinchanaServiceHandler callBack) {
		this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, callBack, false);
	}

	public byte[] discoverService(byte[] key) {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		return this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, null, true).data;
	}

	public void discoverService(byte[] key, SinchanaServiceHandler callBack) {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		this.clientHandler.addRequest(formattedKey, null, MessageType.GET_DATA, callBack, false);
	}

	public void publishService(byte[] key, SinchanaServiceInterface ssi) {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		byte[] formattedReference = ByteArrays.arrayConcat(this.getServerId(), formattedKey);
		boolean success = this.sinchanaServiceStore.publishService(formattedKey, ssi);
		if (success) {
			this.clientHandler.addRequest(formattedKey, formattedReference, MessageType.STORE_DATA, ssi, false);
		} else {
			ssi.isPublished(key, false);
		}
	}

	public void removeService(byte[] key, SinchanaServiceInterface ssi) {
		byte[] formattedKey = ByteArrays.arrayConcat(key, CONFIGURATIONS.SERVICE_TAG);
		this.clientHandler.addRequest(formattedKey, null, MessageType.DELETE_DATA, ssi, false);
	}
}