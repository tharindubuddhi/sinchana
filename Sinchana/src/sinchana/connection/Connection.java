/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.CONFIGURATIONS;
import sinchana.thrift.DHTServer;
import sinchana.thrift.DHTServer.Client;
import sinchana.thrift.Message;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class Connection {

	private long lastUsedTime = -1;
	private long lastOpenTime = -1;
	private long lastKnownSuccessConnectTime = -1;
	private long lastKnownFailedTime = -1;
	private long lastHeardFailedTime = -1;
	private long roundTripTime = -1;
	private int numOfOpenTries = 0;
	private Node node;
	private DHTServer.Client client;
	private TTransport transport;
	private TProtocol protocol;
	private boolean opened = false;
	private boolean failed = false;
	private boolean joinedBackSinceLastFail = false;

	public Connection(Node node) {
		this.node = node;
	}

	public void open() {
		if (opened) {
			return;
		}
		lastOpenTime = System.currentTimeMillis();
		try {
			transport = new TSocket(node.address.split(":")[0],
					Integer.parseInt(node.address.split(":")[1]));
			transport.open();
			protocol = new TBinaryProtocol(transport);
			client = new DHTServer.Client(protocol);
			numOfOpenTries = 0;
			lastKnownSuccessConnectTime = lastOpenTime;
			opened = true;
			failed = false;
		} catch (TTransportException ex) {
			lastKnownFailedTime = lastOpenTime;
			numOfOpenTries++;
		} catch (NullPointerException ex) {
			System.out.println("errrrrrrrr........................ " + transport.toString());
			ex.printStackTrace();
		}
	}

	public boolean isAlive() {
		boolean prevOpened = opened;
		open();
		if (!opened) {
			return false;
		}
		long st = System.currentTimeMillis();
		try {
			client.ping();
			failed = false;
		} catch (TException ex) {
			lastKnownFailedTime = st;
			numOfOpenTries++;
			close();
			return false;
		}
		long et = System.currentTimeMillis();
		roundTripTime = et - st;
		if (!prevOpened) {
			transport.close();
			opened = false;
		}
		return true;
	}

	public void close() {
		if (transport != null && transport.isOpen()) {
			transport.close();
		}
		opened = false;
	}

	public void reset() {
		close();
		numOfOpenTries = 0;
		failed = false;
	}

	public void failed() {
		lastKnownFailedTime = System.currentTimeMillis();
		numOfOpenTries++;
		failed = true;
		close();
	}

	void failedByInfo() {
		lastHeardFailedTime = System.currentTimeMillis();
		failed = true;
		close();
	}

	public long getLastUsedTime() {
		return lastUsedTime;
	}

	public long getLastOpenTime() {
		return lastOpenTime;
	}

	public Client getClient() {
		lastUsedTime = System.currentTimeMillis();
		return client;
	}

	public int getNumOfOpenTries() {
		return numOfOpenTries;
	}

	public boolean isOpened() {
		return opened;
	}

	public boolean isJoinedBackSinceLastFail() {
		return joinedBackSinceLastFail;
	}

	public long getLastHeardFailedTime() {
		return lastHeardFailedTime;
	}

	public long getLastKnownFailedTime() {
		return lastKnownFailedTime;
	}

	public long getLastKnownSuccessConnectTime() {
		return lastKnownSuccessConnectTime;
	}

	public long getRoundTripTime() {
		return roundTripTime;
	}

	public boolean isFailed() {
		return failed;
	}

	public Node getNode() {
		return node;
	}
}
