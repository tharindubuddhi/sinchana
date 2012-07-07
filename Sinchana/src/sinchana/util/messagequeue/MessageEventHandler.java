/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.messagequeue;

import sinchana.thrift.Message;

/**
 *
 * @author Hiru
 */
public interface MessageEventHandler {
		public abstract void process(Message message);		
}
