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

		public abstract void startServer();

		public abstract void stopServer();

		public abstract int send(Message message, Node destination);
}