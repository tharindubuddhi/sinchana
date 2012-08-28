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

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import sinchana.ClientHandler.ClientData;
import sinchana.exceptions.SinchanaInterruptedException;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;
import sinchana.thrift.Response;
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
	public Response discoverService(ByteBuffer serviceKey) throws TException {
		byte[] formattedKey = ByteArrays.arrayConcat(serviceKey.array(), SinchanaDHT.SERVICE_TAG);
		try {
			ClientData cd = server.getClientHandler().addRequest(formattedKey, null,
					MessageType.GET_DATA, -1, null);
			Response response = new Response(cd.success);
			if(cd.success && cd.data != null){
				response.setData(ByteBuffer.wrap(cd.data));
			}else if(!cd.success && cd.error != null){
				response.setData(ByteBuffer.wrap(cd.error));
			}
			return response;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public Response invokeService(ByteBuffer reference, ByteBuffer data) throws TException {
		try {
			ClientData cd = server.getClientHandler().addRequest(reference.array(), data.array(),
											MessageType.GET_SERVICE, -1, null);
			Response response = new Response(cd.success);
			if(cd.success && cd.data != null){
				response.setData(ByteBuffer.wrap(cd.data));
			}else if(!cd.success && cd.error != null){
				response.setData(ByteBuffer.wrap(cd.error));
			}
			return response;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public Response publishData(ByteBuffer dataKey, ByteBuffer data) throws TException {
		try {
			ClientData cd = server.getClientHandler().addRequest(dataKey.array(), 
					data.array(), MessageType.STORE_DATA, -1, null);
			Response response = new Response(cd.success);
			if(cd.success && cd.data != null){
				response.setData(ByteBuffer.wrap(cd.data));
			}else if(!cd.success && cd.error != null){
				response.setData(ByteBuffer.wrap(cd.error));
			}
			return response;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public Response removeData(ByteBuffer dataKey) throws TException {
		try {
			ClientData cd = server.getClientHandler().addRequest(
					dataKey.array(), null, MessageType.DELETE_DATA, -1, null);
			Response response = new Response(cd.success);
			if(cd.success && cd.data != null){
				response.setData(ByteBuffer.wrap(cd.data));
			}else if(!cd.success && cd.error != null){
				response.setData(ByteBuffer.wrap(cd.error));
			}
			return response;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public Response getData(ByteBuffer dataKey) throws TException {
		try {
			ClientData cd = server.getClientHandler().addRequest(
					dataKey.array(), null, MessageType.GET_DATA, -1, null);
			Response response = new Response(cd.success);
			if(cd.success && cd.data != null){
				response.setData(ByteBuffer.wrap(cd.data));
			}else if(!cd.success && cd.error != null){
				response.setData(ByteBuffer.wrap(cd.error));
			}
			return response;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public Response sendRequest(ByteBuffer destination, ByteBuffer message) throws TException {
		try {
			ClientData cd = server.getClientHandler().addRequest(
					destination.array(), message.array(), MessageType.REQUEST, -1, null);
			Response response = new Response(cd.success);
			if(cd.success && cd.data != null){
				response.setData(ByteBuffer.wrap(cd.data));
			}else if(!cd.success && cd.error != null){
				response.setData(ByteBuffer.wrap(cd.error));
			}
			return response;
		} catch (InterruptedException ex) {
			throw new SinchanaInterruptedException(ex);
		}
	}

	@Override
	public void ping() throws TException {
	}
}
