/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import sinchana.SinchanaServer;
import sinchana.service.SinchanaServiceHandler;
import sinchana.util.tools.ByteArrays;

/**
 *
 * @author Hiru
 */
public class ServiceTestServer {

	public static void main(String[] args) throws InterruptedException {
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
		reference = sinchanaServer3.discoverService("HelloService".getBytes());
		if (reference != null) {
			System.out.println("Found at " + ByteArrays.toReadableString(reference));
			resp = sinchanaServer3.getService(reference, "Hiru".getBytes());
			if (resp != null) {
				System.out.println("resp: " + new String(resp));
			} else {
				System.out.println("Service invocation is failed");
			}
		} else {
			System.out.println("Not found!");
		}
		reference = sinchanaServer3.discoverService("GreetingsService".getBytes());
		if (reference != null) {
			System.out.println("Found at " + ByteArrays.toReadableString(reference));
			resp = sinchanaServer3.getService(reference, "Hiru".getBytes());
			if (resp != null) {
				System.out.println("resp: " + new String(resp));
			} else {
				System.out.println("Service invocation is failed");
			}
		} else {
			System.out.println("Not found!");
		}
		SinchanaServiceHandler ssh = new SinchanaServiceHandler() {

			@Override
			public void serviceFound(byte[] key, boolean success, byte[] data) {
				if (success) {
					System.out.println(new String(key) + " is found at " + new String(data));
					sinchanaServer3.getService(data, "Hiru".getBytes(), this);
				} else {
					System.out.println(new String(key) + " is not found");
				}
			}

			@Override
			public void serviceResponse(byte[] key, boolean success, byte[] data) {
				System.out.println("resp: " + new String(data));
			}
		};
		sinchanaServer3.discoverService("HelloService".getBytes(), ssh);
		System.out.println("done :)");
	}
}
