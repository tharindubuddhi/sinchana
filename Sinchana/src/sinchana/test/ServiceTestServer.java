/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import sinchana.SinchanaServer;
import sinchana.exceptions.SinchanaInterruptedException;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.service.SinchanaServiceHandler;
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class ServiceTestServer {

	public static void main(String[] args) throws InterruptedException, TTransportException, TException {
		SinchanaServer sinchanaServer1 = new SinchanaServer("127.0.0.1:9227");
		sinchanaServer1.startServer();
		sinchanaServer1.join();
		System.out.println("S1: " + sinchanaServer1.getServerIdAsString() + " joined the ring");

		SinchanaServer sinchanaServer2 = new SinchanaServer("127.0.0.1:9228", "127.0.0.1:9227");
		sinchanaServer2.startServer();
		sinchanaServer2.join();
		System.out.println("S2: " + sinchanaServer2.getServerIdAsString() + " joined the ring");

		final SinchanaServer sinchanaServer3 = new SinchanaServer("127.0.0.1:9229", "127.0.0.1:9227");
		sinchanaServer3.startServer();
		sinchanaServer3.join();
		System.out.println("S3: " + sinchanaServer3.getServerIdAsString() + " joined the ring");

		sinchanaServer3.testRing();

		sinchanaServer2.publishService("HelloService".getBytes(), new HelloService());
		sinchanaServer2.publishService("GreetingsService".getBytes(), new HelloService());

		Thread.sleep(2000);
		System.out.println("proceed...");
		byte[] reference, resp;
		try {
			reference = sinchanaServer3.discoverService("HelloService".getBytes());
			if (reference != null) {
				System.out.println("Found at " + ByteArrays.toReadableString(reference));
				resp = sinchanaServer3.getService(reference, "Sinchana".getBytes());
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
		} catch (SinchanaInterruptedException sie) {
			System.out.println(sie.getMessage());
		}
		try {
			reference = sinchanaServer3.discoverService("GreetingsService".getBytes());
			if (reference != null) {
				System.out.println("Found at " + ByteArrays.toReadableString(reference));
				resp = sinchanaServer3.getService(reference, "Sinchana".getBytes());
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
		} catch (SinchanaInterruptedException sie) {
			System.out.println(sie.getMessage());
		}
		SinchanaServiceHandler ssh = new SinchanaServiceHandler() {

			@Override
			public void serviceFound(byte[] key, boolean success, byte[] data) {
				if (success) {
					System.out.println(new String(key) + " is found at " + new String(data));
					try {
						sinchanaServer3.getService(data, "Sinchana".getBytes(), this);
					} catch (InterruptedException ex) {
						Logger.getLogger(ServiceTestServer.class.getName()).log(Level.SEVERE, null, ex);
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
		sinchanaServer3.discoverService("HelloService".getBytes(), ssh);
		System.out.println("done :)");
	}
}
