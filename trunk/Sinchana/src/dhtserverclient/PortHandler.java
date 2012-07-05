/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dhtserverclient;

import dhtserverclient.thrift.Message;

/**
 *
 * @author Hiru
 */
public interface PortHandler {

		public abstract void startServer();

		public abstract void stopServer();

		public abstract boolean send(Message message, String address, int portId);
}