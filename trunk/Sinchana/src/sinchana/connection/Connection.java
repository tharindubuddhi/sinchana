/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.connection;

import java.util.Calendar;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.CONFIGURATIONS;
import sinchana.PortHandler;
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
		lastOpenTime = Calendar.getInstance().getTimeInMillis();
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
			System.out.println("errrrrrrrr........................ " + node);
			ex.printStackTrace();
		}
	}

	private int transmit(Message message) {
		open();
		if (!opened) {
			if (numOfOpenTries >= CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
				failed();
				return PortHandler.REMOTE_SERVER_ERROR_FAILURE;
			}
			return PortHandler.REMOTE_SERVER_ERROR;
		}
		long st = Calendar.getInstance().getTimeInMillis();
		int resp;
		try {
			resp = client.transfer(message);
			failed = false;
		} catch (TException ex) {
			lastKnownFailedTime = st;
			numOfOpenTries++;
			close();
			if (numOfOpenTries >= CONFIGURATIONS.NUM_OF_MAX_CONNECT_RETRIES) {
				failed();
				return PortHandler.REMOTE_SERVER_ERROR_FAILURE;
			}
			return PortHandler.REMOTE_SERVER_ERROR;
		}
		long et = Calendar.getInstance().getTimeInMillis();
		roundTripTime = et - st;
		return resp;
	}

	public boolean isAlive() {
		boolean prevOpened = opened;
		open();
		if (!opened) {
			return false;
		}
		long st = Calendar.getInstance().getTimeInMillis();
		try {
			client.ping();
			failed = false;
		} catch (TException ex) {
			lastKnownFailedTime = st;
			numOfOpenTries++;
			close();
			return false;
		}
		long et = Calendar.getInstance().getTimeInMillis();
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
		lastKnownFailedTime = Calendar.getInstance().getTimeInMillis();
		numOfOpenTries++;
		failed = true;
		close();
	}

	void failedByInfo() {
		lastHeardFailedTime = Calendar.getInstance().getTimeInMillis();
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
		lastUsedTime = Calendar.getInstance().getTimeInMillis();
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
