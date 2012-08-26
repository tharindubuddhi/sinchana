package sinchana.test;

import java.math.BigInteger;
import java.util.Random;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import sinchana.CONFIGURATIONS;
import sinchana.SinchanaRequestCallback;
import sinchana.SinchanaResponseCallback;
import sinchana.SinchanaServer;
import sinchana.exceptions.SinchanaInvalidRoutingAlgorithmException;
import sinchana.util.tools.Hash;

public class TestClass {

	private static final String localAddress = "127.0.0.1";
	private static final int port = 6000;
	private static final byte[] message = "Hi".getBytes();
	private static final byte[] response = "Hello".getBytes();
	private static final Random random = new Random();
	private static final SinchanaServer[] servers = new SinchanaServer[10];
	private static final SinchanaRequestCallback requestCallback = new SinchanaRequestCallback() {

		@Override
		public synchronized byte[] request(byte[] message) {
			return response;
		}
	};
	private static final SinchanaResponseCallback responseCallback = new SinchanaResponseCallback() {

		@Override
		public synchronized void response(byte[] message) {
			if (++count % 1000 == 0) {
				end = System.currentTimeMillis();
				System.out.println(count + "\t" + (end - start));
			}
		}

		@Override
		public synchronized void error(byte[] error) {
			throw new UnsupportedOperationException("Not supported yet.");
		}
	};
	private static long start, end, count = 0;

	public static void main(String[] args) throws TTransportException, InterruptedException, TException, SinchanaInvalidRoutingAlgorithmException {

		for (int i = 0; i < servers.length; i++) {
			servers[i] = new SinchanaServer(localAddress + ":" + (port + i), CONFIGURATIONS.PASTRY);
			servers[i].registerSinchanaRequestCallback(requestCallback);
			servers[i].startServer();
			servers[i].join(localAddress + ":" + port);
		}

		start = System.currentTimeMillis();

		for (int i = 0; i < 1000000; i++) {
			String val = new BigInteger(160, random).toString(16);
			int id = (int) (Math.random() * servers.length);
			servers[id].sendRequest(Hash.generateId(val), message, responseCallback);
		}
	}
}
