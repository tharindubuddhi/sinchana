package sinchana.test;

import java.net.UnknownHostException;
import sinchana.SinchanaServer;
import sinchana.SinchanaRequestHandler;
import sinchana.SinchanaResponseHandler;

public class TestClass {

	public static void main(String[] args) throws InterruptedException, UnknownHostException {

		String localAddress = "127.0.0.1";
		
		final SinchanaServer sinchanaServer = new SinchanaServer(localAddress + ":" + 2000);

		sinchanaServer.registerSinchanaRequestHandler(new SinchanaRequestHandler() {

			@Override
			public byte[] request(byte[] message) {
				System.out.println("S1B: " + new String(message));
				return ("Hi " + new String(message)).getBytes();
			}
		});

		System.out.println("starting " + sinchanaServer.getNode().getServerId());
		sinchanaServer.startServer();
		sinchanaServer.join();


		final SinchanaServer sinchanaServer2 = new SinchanaServer(localAddress + ":" + 2001, localAddress + ":" + 2000);

		sinchanaServer2.registerSinchanaRequestHandler(new SinchanaRequestHandler() {

			@Override
			public byte[] request(byte[] message) {
				System.out.println("S2B: " + new String(message));
				return ("Hi " + new String(message)).getBytes();
			}
		});

		System.out.println("starting " + sinchanaServer2.getNode().getServerId());
		sinchanaServer2.startServer();
		sinchanaServer2.join();
		sinchanaServer2.request(sinchanaServer.getNode().getServerId(), "Hello".getBytes(), null);
		sinchanaServer2.request(sinchanaServer.getNode().getServerId(), "Hello".getBytes(), new SinchanaResponseHandler() {

			@Override
			public void response(byte[] message) {
				System.out.println("S2S: " + new String(message));
			}

			@Override
			public void error(byte[] message) {
				throw new UnsupportedOperationException("Not supported yet.");
			}
		});
		System.out.println("passed ;)");
		byte[] resp = sinchanaServer2.request(sinchanaServer.getNode().getServerId(), "Hello".getBytes());
		sinchanaServer2.request(sinchanaServer.getNode().getServerId(), "Hello".getBytes(), null);
		System.out.println("done :) " + new String(resp));
	}
}
