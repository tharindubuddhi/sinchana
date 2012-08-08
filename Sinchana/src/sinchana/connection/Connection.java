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
import sinchana.thrift.DHTServer.Client;

/**
 *
 * @author Hiru
 */
public class Connection {

	private long lastUsedTime = 0;
	private long lastOpenTime = 0;
	private int numOfOpenTries = 0;
	private String url;
	private DHTServer.Client client;
	private TTransport transport;
	private TProtocol protocol;
	private boolean opened = false;

	public Connection(String url) {
		this.url = url;
	}

	public void open() {
		lastOpenTime = Calendar.getInstance().getTimeInMillis();
		try {
			transport = new TSocket(url.split(":")[0], Integer.parseInt(url.split(":")[1]));
			transport.open();
			protocol = new TBinaryProtocol(transport);
			client = new DHTServer.Client(protocol);
			numOfOpenTries = 0;
			opened = true;
		} catch (TTransportException ex) {
			numOfOpenTries++;
			opened = false;
		}
	}

	public void close() {
		if (transport != null && transport.isOpen()) {
			transport.close();
		}
		opened = false;
	}

	public long getLastUsedTime() {
		return lastUsedTime;
	}

	public long getLastOpenTime() {
		return lastOpenTime;
	}

	public Client getClient() {
		lastUsedTime = Calendar.getInstance().getTimeInMillis();
		return client;
	}

	public int getNumOfOpenTries() {
		return numOfOpenTries;
	}

	public boolean isOpened() {
		return opened;
	}
}
