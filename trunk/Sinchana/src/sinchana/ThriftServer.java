/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.thrift.Message;
import sinchana.thrift.DHTServer;
import sinchana.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author Hiru
 */
public class ThriftServer implements DHTServer.Iface, Runnable, PortHandler {

		private Server server;
		private TServer tServer;
		private boolean running;

		public ThriftServer(Server server) {
				this.server = server;
				this.running = true;
		}

		/**
		 * This method will be called when a message is received and the message 
		 * will be passed as the argument. 
		 * @param message Message transfered to the this node.
		 * @throws TException
		 */
		@Override
		public boolean transfer(Message message) throws TException {
				return this.server.getMessageHandler().queueMessage(message);
		}

		@Override
		public void run() {
				try {
						DHTServer.Processor processor = new DHTServer.Processor(this);
						TServerTransport serverTransport = new TServerSocket(this.server.getPortId());
						tServer = new TThreadPoolServer(
								new TThreadPoolServer.Args(serverTransport).processor(processor));
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 0,
								"Starting the server on port " + this.server.getPortId());
						this.running = true;
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setServerIsRunning(true);
						}
						tServer.serve();
						this.running = false;
						if (this.server.getSinchanaTestInterface() != null) {
								this.server.getSinchanaTestInterface().setServerIsRunning(false);
						}
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 1,
								"Server is shutting down...");
				} catch (TTransportException ex) {
						ex.printStackTrace();
				}
		}

		@Override
		public void startServer() {
				if (!this.running) {
						new Thread(this).start();
				}
		}

		@Override
		public void stopServer() {
				if (this.running && this.tServer != null) {
						tServer.stop();
				}
		}

		@Override
		public synchronized boolean send(Message message, String address, int portId) {
				message.lifetime--;
				if (message.lifetime < 0) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 3,
								"Messaage " + message + " is terminated as lifetime expired!");
						return false;
				}
				message.station = this.server;
				TTransport transport = new TSocket(address, portId);
				try {
						transport.open();
						TProtocol protocol = new TBinaryProtocol(transport);
						DHTServer.Client client = new DHTServer.Client(protocol);
						return client.transfer(message);
				} catch (TTransportException ex) {
						Logger.log(this.server.serverId, Logger.LEVEL_WARNING, Logger.CLASS_THRIFT_SERVER, 4,
								"Falied to connect " + address + ":" + portId + " " + ex.getMessage() + " :: " + message);
						return false;
				} catch (TException ex) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 5,
								"Falied to connect 1 " + address + ":" + portId);
						return false;
				} catch (Exception ex) {
						Logger.log(this.server.serverId, Logger.LEVEL_INFO, Logger.CLASS_THRIFT_SERVER, 6,
								"Falied to connect 2 " + address + ":" + portId);
						ex.printStackTrace();
						return false;
				} finally {
						transport.close();
				}

		}
}
