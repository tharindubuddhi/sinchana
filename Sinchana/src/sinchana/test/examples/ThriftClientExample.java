/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test.examples;

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.DHTServer;
import sinchana.util.tools.Hash;

/**
 *
 * @author S.A.H.S.Subasinghe <hirantha.subasinghe@gmail.com>
 */
public class ThriftClientExample {

	private static final String SINCHANA_NODE_ADDRESS = "127.0.0.1";
	private static final int SINCHANA_NODE_PORT = 8000;

	public static void main(String[] args) throws TTransportException, TException {
		/*Opening Thrift connection to the Sinchana node*/
		TTransport transport = new TSocket(SINCHANA_NODE_ADDRESS, SINCHANA_NODE_PORT);
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		DHTServer.Client client = new DHTServer.Client(protocol);

		/*Destination to send the message. It should be exactly 20byte length*/
		byte[] destination = Hash.generateId("Random destination string");
		/*Message to send to the destination*/
		byte[] message = "Hello, world!".getBytes();
		/*Sending request*/
		ByteBuffer response = client.sendRequest(ByteBuffer.wrap(destination), ByteBuffer.wrap(message));
		/*Printing response*/
		System.out.println(new String(response.array()));

		/*Creating some data to publish*/
		byte[] key0 = "k0".getBytes(), data0 = "This is data zero!".getBytes();
		byte[] key1 = "k1".getBytes(), data1 = "This is data one!".getBytes();
		byte[] key2 = "k2".getBytes(), data2 = "This is data two!".getBytes();
		boolean success;
		/*publishing first data set*/
		success = client.publishData(ByteBuffer.wrap(key0), ByteBuffer.wrap(data0));
		if (success) {
			System.out.println("k0 published successfully");
		} else {
			System.out.println("k0 publishing failed");
		}
		/*publishing second data set*/
		success = client.publishData(ByteBuffer.wrap(key1), ByteBuffer.wrap(data1));
		if (success) {
			System.out.println("k1 published successfully");
		} else {
			System.out.println("k1 publishing failed");
		}
		/*publishing third data set*/
		success = client.publishData(ByteBuffer.wrap(key2), ByteBuffer.wrap(data2));
		if (success) {
			System.out.println("k2 published successfully");
		} else {
			System.out.println("k2 publishing failed");
		}
		/*Deleting second data set*/
		success = client.removeData(ByteBuffer.wrap(key1));
		if (success) {
			System.out.println("k1 removed successfully");
		} else {
			System.out.println("k1 removing failed");
		}
	}
}
