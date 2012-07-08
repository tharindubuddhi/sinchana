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
		
		public static final int SUCCESS = 0;
		public static final int ACCEPT_ERROR = 1;
		public static final int REMOTE_SERVER_ERROR = 2;
		public static final int LOCAL_SERVER_ERROR = 3;
		public static final int MESSAGE_LIFE_TIME_EXPIRED = 4;
		

		public abstract void startServer();

		public abstract void stopServer();

		public abstract void send(Message message, Node destination);
}