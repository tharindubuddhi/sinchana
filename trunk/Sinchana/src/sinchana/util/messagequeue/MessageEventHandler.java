/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.messagequeue;

import sinchana.thrift.Message;

/**
 * The event handler interface for MessageQueue.
 * @author Hiru
 */
public interface MessageEventHandler {
		/**
		 * This method will be called when the message queue serves messages.
		 * @param message		The message currently serving.
		 */
		public abstract void process(Message message);		
}
