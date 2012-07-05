/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.thrift.Message;

/**
 *
 * @author Hiru
 */
public interface MessageHandler {
		
		abstract long init();		
		abstract boolean queueMessage(Message message);
}
