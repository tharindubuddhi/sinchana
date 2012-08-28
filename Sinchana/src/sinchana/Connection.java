/************************************************************************************

 * Sinchana Distributed Hash table 

 * Copyright (C) 2012 Sinchana DHT - Department of Computer Science &               
 * Engineering, University of Moratuwa, Sri Lanka. Permission is hereby 
 * granted, free of charge, to any person obtaining a copy of this 
 * software and associated documentation files of Sinchana DHT, to deal 
 * in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.

 * Neither the name of University of Moratuwa, Department of Computer Science 
 * & Engineering nor the names of its contributors may be used to endorse or 
 * promote products derived from this software without specific prior written 
 * permission.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
 * SOFTWARE.                                                                    
 ************************************************************************************/
package sinchana;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.DHTServer;
import sinchana.thrift.DHTServer.Client;
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
	private boolean firstUse = true;
	private boolean joinedBackSinceLastFail = false;

	Connection(Node node) {
		this.node = node;
	}

	void open() throws TTransportException {
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
			throw ex;
		}
	}

	boolean isAlive() {
		boolean prevOpened = opened;
		try {
			open();
			if (!opened) {
				return false;
			}
			long st = System.currentTimeMillis();
			client.ping();
			roundTripTime = System.currentTimeMillis() - st;
			lastKnownSuccessConnectTime = st;
			numOfOpenTries = 0;
			failed = false;
		} catch (TException ex) {
			lastKnownFailedTime = System.currentTimeMillis();
			lastHeardFailedTime = lastKnownFailedTime;
			numOfOpenTries++;
			close();
			return false;
		}
		if (!prevOpened) {
			transport.close();
			opened = false;
		}
		return true;
	}

	void close() {
		if (transport != null && transport.isOpen()) {
			transport.close();
		}
		opened = false;
	}

	void reset() {
		close();
		numOfOpenTries = 0;
		failed = false;
	}

	void failed() {
		lastKnownFailedTime = System.currentTimeMillis();
		lastHeardFailedTime = lastKnownFailedTime;
		numOfOpenTries++;
		close();
	}

	void failedPermenently() {
		lastKnownFailedTime = System.currentTimeMillis();
		lastHeardFailedTime = lastKnownFailedTime;
		numOfOpenTries++;
		failed = true;
		close();
	}

	boolean updateInfo(boolean alive) {
		if (alive) {
			if (firstUse) {
				firstUse = false;
				return isAlive();
			}
			if (!failed || !isAlive()) {
				return false;
			}
			numOfOpenTries = 0;
			failed = false;
		} else {
			if (firstUse) {
				firstUse = false;
				return !isAlive();
			}			
			if (failed || isAlive()) {
				return false;
			}
			lastHeardFailedTime = System.currentTimeMillis();
			failed = true;
		}
		return true;
	}

	long getLastUsedTime() {
		return lastUsedTime;
	}

	long getLastOpenTime() {
		return lastOpenTime;
	}

	Client getClient() {
		lastUsedTime = System.currentTimeMillis();
		return client;
	}

	int getNumOfOpenTries() {
		return numOfOpenTries;
	}

	boolean isOpened() {
		return opened;
	}

	boolean isJoinedBackSinceLastFail() {
		return joinedBackSinceLastFail;
	}

	long getLastHeardFailedTime() {
		return lastHeardFailedTime;
	}

	long getLastKnownFailedTime() {
		return lastKnownFailedTime;
	}

	long getLastKnownSuccessConnectTime() {
		return lastKnownSuccessConnectTime;
	}

	long getRoundTripTime() {
		return roundTripTime;
	}

	boolean isFailed() {
		return failed;
	}

	Node getNode() {
		return node;
	}
}
