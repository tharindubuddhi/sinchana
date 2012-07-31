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

				final Server sinchanaServer = new Server(ip[0], (short) 2000);

				sinchanaServer.registerSinchanaInterface(new SinchanaInterface() {

						@Override
						public Message receive(Message message) {

								System.out.println("S1: " + message);

								Message message2 = new Message(sinchanaServer, MessageType.ACCEPT, 1);

								message2.setMessage("S1: " + "response form " + message.message);

								return message2;

						}
				});

				System.out.println("starting " + sinchanaServer.getServerId());

				sinchanaServer.startServer();

				Thread.sleep(1000);

				sinchanaServer.join();



				final Server sinchanaServer2 = new Server(ip[0], (short) 2001);

				sinchanaServer2.registerSinchanaInterface(new SinchanaInterface() {

						@Override
						public Message receive(Message message) {

								System.out.println("S2: " + message);

								return null;

						}
				});

				Node node = new Node();

				node.setAddress("127.0.0.1");

				node.setPortId((short) 2000);

				sinchanaServer2.setAnotherNode(node);

				System.out.println("startoing " + sinchanaServer2.getServerId());

				sinchanaServer2.startServer();

				Thread.sleep(1000);



				sinchanaServer2.join();

				Thread.sleep(1000);

//				Message msg = new Message(sinchanaServer2, MessageType.TEST_RING, 1024);
//				msg.setMessage("");
//				sinchanaServer2.send(msg);



				Message message2 = new Message(sinchanaServer2, MessageType.GET, 10);

				message2.setMessage("Hello");

				message2.setTargetKey(sinchanaServer.getServerId());

				sinchanaServer2.send(message2);

		}
}
