package sinchana.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import sinchana.Server;
import sinchana.SinchanaInterface;
import sinchana.SinchanaStoreInterface;
import sinchana.thrift.DataObject;
import sinchana.thrift.Message;
import sinchana.thrift.MessageType;

public class TestClass {

	public static void main(String[] args) throws InterruptedException, UnknownHostException {

//		InetAddress[] ip = InetAddress.getAllByName("localhost");
//		String localAddress = ip[0].getHostAddress();
        String localAddress = InetAddress.getLocalHost().getHostAddress();

		final Server sinchanaServer = new Server(localAddress + ":" + 2000);

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
		sinchanaServer.join();


		final Server sinchanaServer2 = new Server(localAddress + ":" + 2001, localAddress + ":" + 2000);

		sinchanaServer2.registerSinchanaInterface(new SinchanaInterface() {

			@Override
			public Message request(Message message) {
				System.out.println("S21: " + message);
				return null;
			}

			@Override
			public void response(Message message) {
				System.out.println("S22: " + message);
			}

			@Override
			public void error(Message message) {
				throw new UnsupportedOperationException("Not supported yet.");
			}
		});

		System.out.println("starting " + sinchanaServer2.getServerId());
		sinchanaServer2.startServer();
		sinchanaServer2.join();
		sinchanaServer2.send(sinchanaServer.getServerId(), "Hello");

        sinchanaServer.storeData("data", new SinchanaStoreInterface() {

            @Override
            public void store(DataObject dataObject) {
                System.out.println("horry data stored in root.. "+dataObject.dataValue);
            }

            @Override
            public void get(Set<DataObject> dataObjectSet) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void remove(DataObject dataObject) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void isStored(Boolean success) {
                System.out.println("horray data stored response came back...");
            }

            @Override
            public void isRemoved(Boolean success) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        });
        
	}
}
