/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.util.tools.CommonTools;

/**
 *
 * @author Hiru
 */
public class ThriftServerImpl implements DHTServer.Iface {

	private final SinchanaServer server;

	public ThriftServerImpl(SinchanaServer svr) {
		this.server = svr;
	}

	/**
	 * This method will be called when a message is received and the message 
	 * will be passed as the argument. 
	 * @param message Message transfered to the this node.
	 * @return 
	 * @throws TException
	 */
	@Override
	public int transfer(Message message) throws TException {
		if (CONFIGURATIONS.ROUND_TRIP_TIME != 0) {
			try {
				Thread.sleep(CONFIGURATIONS.ROUND_TRIP_TIME);
			} catch (InterruptedException ex) {
				throw new RuntimeException(ex);
			}
		}
		if (server.getMessageHandler().queueMessage(message)) {
			return PortHandler.SUCCESS;
		}
		return PortHandler.ACCEPT_ERROR;
	}

	@Override
	public ByteBuffer discoverService(ByteBuffer serviceKey) throws TException {
		byte[] formattedKey = CommonTools.arrayConcat(serviceKey.array(), CONFIGURATIONS.SERVICE_TAG);
		return ByteBuffer.wrap(server.getClientHandler().addRequest(formattedKey, null,
				MessageType.GET_DATA, null, true).data);
	}

	@Override
	public ByteBuffer getService(ByteBuffer reference, ByteBuffer data) throws TException {
		return ByteBuffer.wrap(server.getClientHandler().addRequest(reference.array(), data.array(),
				MessageType.GET_SERVICE, null, true).data);
	}

	@Override
	public boolean publishData(ByteBuffer dataKey, ByteBuffer data) throws TException {
		return server.getClientHandler().addRequest(dataKey.array(), data.array(), MessageType.STORE_DATA, null, true).success;
	}

	@Override
	public boolean removeData(ByteBuffer dataKey) throws TException {
		return server.getClientHandler().addRequest(dataKey.array(), null, MessageType.DELETE_DATA, null, true).success;
	}

	@Override
	public ByteBuffer getData(ByteBuffer dataKey) throws TException {
		return ByteBuffer.wrap(server.getClientHandler().addRequest(dataKey.array(), null, MessageType.GET_DATA, null, true).data);
	}

	@Override
	public ByteBuffer request(ByteBuffer destination, ByteBuffer message) throws TException {
		return ByteBuffer.wrap(server.getClientHandler().addRequest(destination.array(), message.array(),
				MessageType.REQUEST, null, true).data);
	}

	@Override
	public void ping() throws TException {
		System.out.println(server.serverId + ": pinged :)");
	}
}
