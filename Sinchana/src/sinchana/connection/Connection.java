/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.Calendar;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.DHTServer;

/**
 *
 * @author Hiru
 */
public class Connection {

		private long lastUsedTime = 0;
		private long lastOpenTime = 0;
		private int numOfOpenTries = 0;
		private String address;
		private int portId;
		private boolean opened;
		private DHTServer.Client client;
		private TTransport transport;
		private TProtocol protocol;

		public Connection(String address, int portId) {
				this.address = address;
				this.portId = portId;
		}

		public DHTServer.Client open() {
				lastUsedTime = Calendar.getInstance().getTimeInMillis();
				if (opened) {
						return client;
				}
				lastOpenTime = lastUsedTime;
				try {
						transport = new TSocket(address, portId);
						transport.open();
						protocol = new TBinaryProtocol(transport);
						client = new DHTServer.Client(protocol);
						opened = true;
						numOfOpenTries = 0;
						return client;
				} catch (TTransportException ex) {
						numOfOpenTries++;
						return null;
				}
		}

		public void reset() {
				if (transport != null && transport.isOpen()) {
						transport.close();
				}
				opened = false;
		}

		public long getLastUsedTime() {
				return lastUsedTime;
		}
}
