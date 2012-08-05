package sinchana.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import sinchana.Server;

import sinchana.SinchanaInterface;

import sinchana.thrift.Message;

import sinchana.thrift.MessageType;

import sinchana.thrift.Node;

public class TestClass {

		public static void main(String[] args) throws InterruptedException, UnknownHostException {



				InetAddress[] ip = InetAddress.getAllByName("localhost");
				System.out.println(ip[0].toString());

				final Server sinchanaServer = new Server((short) 2000);

				sinchanaServer.registerSinchanaInterface(new SinchanaInterface() {

						@Override
						public Message request(Message message) {
								System.out.println("S1: " + message);
								Message message2 = new Message(sinchanaServer, MessageType.RESPONSE, 1);
								message2.setMessage("S1: " + "response form " + message.message);
								return message2;
						}

						@Override
						public void response(Message message) {
								throw new UnsupportedOperationException("Not supported yet.");
						}

						@Override
						public void error(Message message) {
								throw new UnsupportedOperationException("Not supported yet.");
						}
				});

				System.out.println("starting " + sinchanaServer.getServerId());

				sinchanaServer.startServer();

				Thread.sleep(1000);

				sinchanaServer.join();



				final Server sinchanaServer2 = new Server((short) 2001);

				sinchanaServer2.registerSinchanaInterface(new SinchanaInterface() {

						@Override
						public Message request(Message message) {
								throw new UnsupportedOperationException("Not supported yet.");
						}

						@Override
						public void response(Message message) {
								throw new UnsupportedOperationException("Not supported yet.");
						}

						@Override
						public void error(Message message) {
								throw new UnsupportedOperationException("Not supported yet.");
						}
				});

				Node node = new Node();

				node.setAddress("127.0.0.1");

				node.setPortId((short) 2000);

				sinchanaServer2.setAnotherNode(node);

				System.out.println("starting " + sinchanaServer2.getServerId());

				sinchanaServer2.startServer();

				Thread.sleep(1000);



				sinchanaServer2.join();

				Thread.sleep(1000);

//				Message msg = new Message(sinchanaServer2, MessageType.TEST_RING, 1024);
//				msg.setMessage("");
//				sinchanaServer2.send(msg);



				Message message2 = new Message(sinchanaServer2, MessageType.REQUEST, 10);

				message2.setMessage("Hello");

				message2.setTargetKey(sinchanaServer.getServerId());

				sinchanaServer2.send(message2);

		}
}
