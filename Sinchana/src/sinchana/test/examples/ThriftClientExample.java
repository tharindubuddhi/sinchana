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
package sinchana.test.examples;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Response;

/**
 *This class illustrates examples to get services from the Sinchana DHT via Thrift clients.
 * @author S.A.H.S.Subasinghe <hirantha.subasinghe@gmail.com>
 */
public class ThriftClientExample {

	/*address of the Sinchana node to connect*/
	private static final String SINCHANA_NODE_ADDRESS = "127.0.0.1";
	/*port id of the sinchana node to connect*/
	private static final int SINCHANA_NODE_PORT = 8000;

	public static void main(String[] args) throws TTransportException, TException {
		/*opening Thrift connection to the Sinchana node*/
		TTransport transport = new TSocket(SINCHANA_NODE_ADDRESS, SINCHANA_NODE_PORT);
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		DHTServer.Client client = new DHTServer.Client(protocol);

		/*destination to send the message. It should be exactly 20byte length*/
		byte[] destination = new BigInteger("64A78FA91EBB01182346712398A1A0B5CDDEDF08", 16).toByteArray();
		/*message to send to the destination*/
		byte[] message = "Hello, world!".getBytes();
		/*sending request*/
		Response response = client.sendRequest(ByteBuffer.wrap(destination), ByteBuffer.wrap(message));
		/*printing response*/
		System.out.println(new String(response.getData()));

		/*Creating some data to publish*/
		byte[] key0 = "k0".getBytes(), data0 = "This is data zero!".getBytes();
		byte[] key1 = "k1".getBytes(), data1 = "This is data one!".getBytes();
		byte[] key2 = "k2".getBytes(), data2 = "This is data two!".getBytes();
		/*publishing first data set*/
		response = client.publishData(ByteBuffer.wrap(key0), ByteBuffer.wrap(data0));
		if (response.success) {
			System.out.println("k0 published successfully");
		} else {
			System.out.println("k0 publishing failed");
		}
		/*publishing second data set*/
		response = client.publishData(ByteBuffer.wrap(key1), ByteBuffer.wrap(data1));
		if (response.success) {
			System.out.println("k1 published successfully");
		} else {
			System.out.println("k1 publishing failed");
		}
		/*publishing third data set*/
		response = client.publishData(ByteBuffer.wrap(key2), ByteBuffer.wrap(data2));
		if (response.success) {
			System.out.println("k2 published successfully");
		} else {
			System.out.println("k2 publishing failed");
		}
		/*deleting second data set*/
		response = client.removeData(ByteBuffer.wrap(key1));
		if (response.success) {
			System.out.println("k1 removed successfully");
		} else {
			System.out.println("k1 removing failed");
		}
		/*retrieving first data set*/
		response = client.getData(ByteBuffer.wrap(key0));
		if (response.success) {
			System.out.println("k0: " + new String(response.getData()));
		} else {
			System.out.println("k0 couldn't find");
		}
		/*retrieving second data set*/
		response = client.getData(ByteBuffer.wrap(key1));
		if (response.success && response.isSetData()) {
			System.out.println("k1: " + new String(response.getData()));
		} else {
			System.out.println("k1 couldn't find");
		}
		/*retrieving third data set*/
		response = client.getData(ByteBuffer.wrap(key2));
		if (response.success) {
			System.out.println("k2: " + new String(response.getData()));
		} else {
			System.out.println("k2 couldn't find");
		}
		/*service key*/
		byte[] serviceKey = "Hello Service".getBytes();
		/*data to be processed by the service*/
		byte[] serviceData = "Data to precess".getBytes();
		/*discover 'Hello Service'*/
		response = client.discoverService(ByteBuffer.wrap(serviceKey));
		if (response.success && response.isSetData()) {
			/*invoke 'Hello Service'*/
			response = client.invokeService(response.data, ByteBuffer.wrap(serviceData));
			/*printing response*/
			System.out.println("Response: " + new String(response.getData()));
		} else {
			System.out.println("Hello Service is not found");
		}


	}
}
