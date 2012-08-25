/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import sinchana.exceptions.SinchanaInterruptedException;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class ThriftServerImpl implements DHTServer.Iface {

	private final SinchanaServer server;

	public ThriftServerImpl(SinchanaServer svr) {
		this.server = svr;
	}

	@Override
	public int transfer(Message message) throws TException {
		message.lifetime--;
		if (server.getMessageHandler().queueMessage(message)) {
			return IOHandler.SUCCESS;
		}
		return IOHandler.ACCEPT_ERROR;
	}

	@Override
	public ByteBuffer discoverService(ByteBuffer serviceKey) throws TException {
		byte[] formattedKey = ByteArrays.arrayConcat(serviceKey.array(), CONFIGURATIONS.SERVICE_TAG);
		try {
			return ByteBuffer.wrap(server.getClientHandler().addRequest(formattedKey, null,
					MessageType.GET_DATA, -1, null).data);
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public ByteBuffer getService(ByteBuffer reference, ByteBuffer data) throws TException {
		try {
			return ByteBuffer.wrap(server.getClientHandler().addRequest(reference.array(), data.array(),
					MessageType.GET_SERVICE, -1, null).data);
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public boolean publishData(ByteBuffer dataKey, ByteBuffer data) throws TException {
		try {
			return server.getClientHandler().addRequest(dataKey.array(), data.array(), MessageType.STORE_DATA, -1, null).success;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public boolean removeData(ByteBuffer dataKey) throws TException {
		try {
			return server.getClientHandler().addRequest(dataKey.array(), null, MessageType.DELETE_DATA, -1, null).success;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public ByteBuffer getData(ByteBuffer dataKey) throws TException {
		try {
			return ByteBuffer.wrap(server.getClientHandler().addRequest(dataKey.array(), null, MessageType.GET_DATA, -1, null).data);
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public ByteBuffer request(ByteBuffer destination, ByteBuffer message) throws TException {
		try {
			return ByteBuffer.wrap(server.getClientHandler().addRequest(destination.array(), message.array(),
					MessageType.REQUEST, -1, null).data);
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public void ping() throws TException {
	}
}
