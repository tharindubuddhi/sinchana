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
public interface SinchanaInterface {

	/**
	 * Receive messages where the destination is this server. They can be either
	 * <p>
	 * MessageType.GET - Resource request, <p>
	 * MessageType.ACCEPT - Responses to resource request or <p>
	 * MessageType.ERROR - Error response to resource requests.
	 * 
	 * @param message		Message.
	 * @return				Return message to send back to the sender. Return null if no messages need to be sent back
	 */
	public abstract Message request(Message message);

	public abstract void response(Message message);

	public abstract void error(Message message);
}
