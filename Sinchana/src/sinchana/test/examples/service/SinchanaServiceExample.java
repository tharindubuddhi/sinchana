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
package sinchana.test.examples.service;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import sinchana.SinchanaDHT;
import sinchana.SinchanaServer;
import sinchana.exceptions.SinchanaInvalidRoutingAlgorithmException;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.service.SinchanaServiceCallback;
import sinchana.util.tools.ByteArrays;

/**
 *This class is to illustrate example of publishing and retrieving services with
 * the Sinchana DHT.
 * @author Hirantha Subasinghe
 */
public class SinchanaServiceExample {

	public static void main(String[] args) throws InterruptedException, 
			TTransportException, TException, SinchanaInvalidRoutingAlgorithmException {
		/*Starting server one*/
		final SinchanaDHT sinchanaServer1 = new SinchanaServer("127.0.0.1:9227", SinchanaDHT.PASTRY);
		sinchanaServer1.startServer();
		sinchanaServer1.join(null);
		System.out.println("Server 1: " + sinchanaServer1.getServerIdAsString() + " joined the ring");

		/*Starting server two*/
		final SinchanaDHT sinchanaServer2 = new SinchanaServer("127.0.0.1:9228", SinchanaDHT.PASTRY);
		sinchanaServer2.startServer();
		sinchanaServer2.join("127.0.0.1:9227");
		System.out.println("Server 2: " + sinchanaServer2.getServerIdAsString() + " joined the ring");

		/*Starting server three*/
		final SinchanaDHT sinchanaServer3 = new SinchanaServer("127.0.0.1:9229", SinchanaDHT.PASTRY);
		sinchanaServer3.startServer();
		sinchanaServer3.join("127.0.0.1:9227");
		System.out.println("Server 3: " + sinchanaServer3.getServerIdAsString() + " joined the ring");

		/*Publishing 'HelloService' in second server*/
		sinchanaServer2.publishService("HelloService".getBytes(), new HelloService());
		/*Publishing 'TimeService' in second server*/
		sinchanaServer2.publishService("TimeService".getBytes(), new TimeService());

		/*Give some time to make sure that services are published*/
		Thread.sleep(1000);
		/*Proceeding to next step*/
		System.out.println("proceed...");
		byte[] reference, resp;
		try {
			/*discovering 'HelloService' synchronously*/
			reference = sinchanaServer3.discoverService("HelloService".getBytes());
			if (reference != null) {
				System.out.println("Found at " + ByteArrays.toReadableString(reference));
				/*invoking 'HelloService' synchronously*/
				resp = sinchanaServer3.invokeService(reference, "Sinchana".getBytes());
				if (resp != null) {
					System.out.println("resp: " + new String(resp));
				} else {
					System.out.println("Service invocation is failed");
				}
			} else {
				System.out.println("Not found!");
			}
		} catch (SinchanaTimeOutException ste) {
			System.out.println(ste.getMessage());
		}
		try {
			/*discovering 'TimeService' synchronously*/
			reference = sinchanaServer3.discoverService("TimeService".getBytes());
			if (reference != null) {
				System.out.println("Found at " + ByteArrays.toReadableString(reference));
				/*invoking 'TimeService' synchronously*/
				resp = sinchanaServer3.invokeService(reference, "Sinchana".getBytes());
				if (resp != null) {
					System.out.println("resp: " + new String(resp));
				} else {
					System.out.println("Service invocation is failed");
				}
			} else {
				System.out.println("Not found!");
			}
		} catch (SinchanaTimeOutException ste) {
			System.out.println(ste.getMessage());
		}
		SinchanaServiceCallback ssh = new SinchanaServiceCallback() {

			@Override
			public void serviceFound(byte[] key, boolean success, byte[] data) {
				if (success) {
					System.out.println(new String(key) + " is found at " + new String(data));
					try {
						/*invoking 'HelloService' asynchronously*/
						sinchanaServer3.invokeService(data, "Sinchana".getBytes(), this);
					} catch (InterruptedException ex) {
						Logger.getLogger(SinchanaServiceExample.class.getName()).log(Level.SEVERE, null, ex);
					}
				} else {
					System.out.println(new String(key) + " is not found");
				}
			}

			@Override
			public void serviceResponse(byte[] key, boolean success, byte[] data) {
				System.out.println("resp: " + new String(data));
			}

			@Override
			public void error(byte[] error) {
				System.out.println("Error: " + new String(error));
			}
		};
		/*discovering 'HelloService' asynchronously*/
		sinchanaServer3.discoverService("HelloService".getBytes(), ssh);
		System.out.println("done :)");
	}
}
