/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.thrift.Message;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public interface PortHandler {
		
		/**
		 * Indicates a successful transmission.
		 */
		public static final int SUCCESS = 0;
		/**
		 * Indicates a accepting failure of the massage at the remote end. 
		 * This is because the input buffer at the remote end is full.
		 */
		public static final int ACCEPT_ERROR = 1;
		/**
		 * Indicates a remote server error. This is because the network is failed 
		 * or the remote server is failed / does not accept connections.
		 */
		public static final int REMOTE_SERVER_ERROR = 2;
		/**
		 * Indicates a local server error. This is because the number of maximum 
		 * connections opened is reached.
		 */
		public static final int LOCAL_SERVER_ERROR = 3;
		/**
		 * Indicates that the message is terminated because it's life time is expired.
		 */
		public static final int MESSAGE_LIFE_TIME_EXPIRED = 4;
		

		/**
		 * Start the server.
		 */
		public abstract void startServer();

		/**
		 * Stop the server.
		 */
		public abstract void stopServer();

		/**
		 * Send a message to the destination.
		 * @param message		Message to send.
		 * @param destination Destination node to send the message.
		 */
		public abstract void send(Message message, Node destination);
}