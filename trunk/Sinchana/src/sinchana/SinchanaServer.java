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
import java.util.Arrays;
import sinchana.chord.ChordTable;
import sinchana.connection.ConnectionPool;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Node;
import sinchana.util.tools.CommonTools;

/**
 *
 * @author S.A.H.S.Subasinghe
 */
public class SinchanaServer extends Node {
	
	public static final BigInteger GRID_SIZE = new BigInteger("2", 16).pow(160);
	private final PortHandler portHandler = new IOHandler(this);
	private final RoutingHandler routingHandler = new ChordTable(this);
	private final MessageHandler messageHandler = new MessageHandler(this);
	private final ConnectionPool connectionPool = new ConnectionPool(this);
	private final ClientHandler clientHandler = new ClientHandler(this);
	private final SinchanaServiceStore sinchanaServiceStore = new SinchanaServiceStore();
	private SinchanaRequestHandler SinchanaRequestHandler = null;
	private SinchanaTestInterface sinchanaTestInterface = null;
	private SinchanaDataStoreInterface sinchanaDataStoreInterface = new SinchanaDataStoreImpl();
	private String remoteNodeAddress = null;
	private BigInteger serverIdAsBigInt;
	private String serverIdAsString;
	private boolean joined = false;

	/**
	 * Start a new node with the given server ID and next hop.
	 * @param serverId		SinchanaServer ID. Generated using a hash function. 
	 * @param anotherNode	Another node is the network. New node first
	 * communicate with this node to discover the rest of the network.
	 * @param address		URL of the server.
	 * @param portId		Port Id number where the the server is running.
	 */
	public SinchanaServer(int localPortId) {
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			this.init(inetAddress.getHostAddress() + ":" + localPortId, null);
		} catch (UnknownHostException ex) {
			throw new RuntimeException("Error getting local host ip.", ex);
		}
	}
	
	public SinchanaServer(String localAddress) {
		this.init(localAddress, null);
	}
	
	public SinchanaServer(int localPortId, String remoteNodeAddress) {
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			this.init(inetAddress.getHostAddress() + ":" + localPortId, remoteNodeAddress);
		} catch (UnknownHostException ex) {
			throw new RuntimeException("Error getting local host ip.", ex);
		}
	}
	
	public SinchanaServer(String localAddress, String remoteNodeAddress) {
		this.init(localAddress, remoteNodeAddress);
	}
	
	private void init(String address, String remoteNodeAddress) {
		this.address = address;
		this.setServerId(CommonTools.generateId(this.address));
		this.serverIdAsBigInt = new BigInteger(this.getServerId());
		this.serverIdAsString = CommonTools.toReadableString(this.getServerId());
		System.out.println("length: " + this.getServerId().length + "\t" + Arrays.toString(this.getServerId()));
		this.remoteNodeAddress = remoteNodeAddress;
	}

	/**
	 * Start the server.
	 */
	public void startServer() {
		this.routingHandler.init();
		this.portHandler.startServer();
	}
	
	public void join() {
		if (this.remoteNodeAddress != null && !this.remoteNodeAddress.equals(this.address)) {
			Message msg = new Message(this, MessageType.JOIN, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
			Node remoteNode = new Node(ByteBuffer.wrap(CommonTools.generateId(remoteNodeAddress)), remoteNodeAddress);
			int count = CONFIGURATIONS.JOIN_RETRY_TIME_OUT * 100;
			while (!joined) {
				try {
					count++;
					if (count > CONFIGURATIONS.JOIN_RETRY_TIME_OUT * 100) {
						count = 0;
						System.out.println("Connecting to " + remoteNodeAddress);
						this.portHandler.send(msg, remoteNode);
					}
					Thread.sleep(10);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
			}
		} else {
			this.messageHandler.startAsRootNode();
			if (this.sinchanaTestInterface != null) {
				this.sinchanaTestInterface.setStable(true);
			}
		}
	}
	
	void setJoined(boolean joined) {
		this.joined = joined;
	}

	/**
	 * Stop the server.
	 */
	public void stopServer() {
		portHandler.stopServer();
		messageHandler.terminate();
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
		Message message = new Message(this, MessageType.TEST_RING, CONFIGURATIONS.DEFAUILT_MESSAGE_LIFETIME);
		message.setStation(this);
		this.getMessageHandler().queueMessage(message);
	}

	/**
	 * Send a MessageType.GET message.
	 * @param destination	Destination ID to receive message.
	 * @param message		Message string.
	 */
	public byte[] request(byte[] destination, byte[] message) {
		return this.clientHandler.addRequest(destination, message, MessageType.REQUEST, null, true, null).data;
	}
	
	public void request(byte[] destination, byte[] message, SinchanaResponseHandler callBack) {
		this.clientHandler.addRequest(destination, message, MessageType.REQUEST, callBack, false, null);
	}
	
	public boolean storeData(byte[] key, byte[] data) {
		return this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, null, true, null).success;
	}
	
	public void storeData(byte[] key, byte[] data, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, data, MessageType.STORE_DATA, callBack, false, null);
	}
	
	public byte[] getData(byte[] key) {
		return this.clientHandler.addRequest(key, null, MessageType.GET_DATA, null, true, null).data;
	}
	
	public void getData(byte[] key, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, null, MessageType.GET_DATA, callBack, false, null);
	}
	
	public boolean deleteData(byte[] key) {
		return this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, null, true, null).success;
	}
	
	public void deleteData(byte[] key, SinchanaDataHandler callBack) {
		this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, callBack, false, null);
	}
	
	public byte[] getService(byte[] reference, byte[] data) {
		return this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, null, true, null).data;
	}
	
	public void getService(byte[] reference, byte[] data, SinchanaServiceHandler callBack) {
		this.clientHandler.addRequest(reference, data, MessageType.GET_SERVICE, callBack, false, null);
	}
	
	public byte[] discoverService(byte[] key) {
		return this.clientHandler.addRequest(key, null, MessageType.GET_DATA, null, true, CONFIGURATIONS.SERVICE_TAG).data;
	}
	
	public void discoverService(byte[] key, SinchanaServiceHandler callBack) {
		this.clientHandler.addRequest(key, null, MessageType.GET_DATA, callBack, false, CONFIGURATIONS.SERVICE_TAG);
	}
	
	public void publishService(byte[] key, byte[] reference, SinchanaServiceInterface ssi) {
		boolean success = this.sinchanaServiceStore.publishService(key, ssi);
		if (success) {
			this.clientHandler.addRequest(key, reference, MessageType.STORE_DATA, ssi, false, CONFIGURATIONS.SERVICE_TAG);
		} else {
			ssi.isPublished(key, false);
		}
	}
	
	public void removeService(byte[] key, SinchanaServiceInterface ssi) {
		this.clientHandler.addRequest(key, null, MessageType.DELETE_DATA, ssi, false, CONFIGURATIONS.SERVICE_TAG);
	}
}
