/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import sinchana.SinchanaServer;
import sinchana.service.SinchanaServiceInterface;

/**
 *
 * @author Hiru
 */
public class ServiceTestServer {

	public static void main(String[] args) throws InterruptedException {
		SinchanaServer sinchanaServer1 = new SinchanaServer("127.0.0.1:9227");
		sinchanaServer1.startServer();
		sinchanaServer1.join();
		System.out.println("S1: " + sinchanaServer1.serverId + " joined the ring");

		SinchanaServer sinchanaServer2 = new SinchanaServer("127.0.0.1:9228", "127.0.0.1:9227");
		sinchanaServer2.startServer();
		sinchanaServer2.join();
		System.out.println("S2: " + sinchanaServer2.serverId + " joined the ring");

		SinchanaServer sinchanaServer3 = new SinchanaServer("127.0.0.1:9229", "127.0.0.1:9227");
		sinchanaServer3.startServer();
		sinchanaServer3.join();
		System.out.println("S3: " + sinchanaServer3.serverId + " joined the ring");

		sinchanaServer3.testRing();

		sinchanaServer2.publishService("HelloService".getBytes(), sinchanaServer2.getServerId(), new SinchanaServiceInterface() {

			@Override
			public byte[] process(byte[] serviceKey, byte[] data) {
				return ("HelloService:: Hi " + new String(data) + ", Greetings from " + serviceKey).getBytes();
			}

			@Override
			public void isPublished(byte[] serviceKey, Boolean success) {
				System.out.println("HelloService is published");
			}

			@Override
			public void isRemoved(byte[] serviceKey, Boolean success) {
				throw new UnsupportedOperationException("Not supported yet.");
			}
		});
		sinchanaServer2.publishService("GreetingsService".getBytes(), sinchanaServer2.getServerId(), new SinchanaServiceInterface() {

			@Override
			public byte[] process(byte[] serviceKey, byte[] data) {
				return ("GreetingsService:: Hi " + new String(data) + ", Greetings from " + serviceKey).getBytes();
			}

			@Override
			public void isPublished(byte[] serviceKey, Boolean success) {
				System.out.println("GreetingsService is published");
			}

			@Override
			public void isRemoved(byte[] serviceKey, Boolean success) {
				throw new UnsupportedOperationException("Not supported yet.");
			}
		});

		Thread.sleep(2000);
		System.out.println("proceed...");
		byte[] reference, resp;
		reference = sinchanaServer3.discoverService("HelloService".getBytes());
		if (reference != null) {
			System.out.println("Found at " + new String(reference));
			resp = sinchanaServer3.getService(reference, "HelloService".getBytes());
			System.out.println(new String(resp));
		} else {
			System.out.println("Not found!");
		}
		reference = sinchanaServer3.discoverService("GreetingsService".getBytes());
		if (reference != null) {
			System.out.println("Found at " + new String(reference));
			resp = sinchanaServer3.getService(reference, "GreetingsService".getBytes());
			System.out.println(new String(resp));
		} else {
			System.out.println("Not found!");
		}
	}
}
