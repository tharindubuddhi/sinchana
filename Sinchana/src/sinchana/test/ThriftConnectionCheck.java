/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Message;

/**
 *
 * @author Hiru
 */
public class ThriftConnectionCheck {

		public static void main(String[] args) {
				try {
						Message message = new Message();
						TTransport transport = new TSocket(
								"127.0.0.1", 2010);
						transport.open();
						TProtocol protocol = new TBinaryProtocol(transport);
						DHTServer.Client client = new DHTServer.Client(protocol);
						client.transfer(message);
				} catch (Exception ex) {
						ex.printStackTrace();
				}
		}
}
